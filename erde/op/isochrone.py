from erde import utils, CONFIG, dprint, autocli, write_df, write_stream
from erde.op.table import table_route
from matplotlib.tri import LinearTriInterpolator, Triangulation
from shapely.geometry import Point, MultiPolygon
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

SNAP_SPEED = 2.5
KMH2MPS = 3.6
MAX_SNAP = 250
GRID_EVERY_N_SECONDS = 30

# default size for OSRM router.
#If you run it yourself without an intermediate HTTP server (nginx), the limit can be safely raised to 100K
MAX_TABLE_SIZE = 2_000

POLY_DURATION_COL = 'duration'
FULL_DURATION = 'full_duration'


def raster2polygons(x, y, z, levels, level_field):
	if 0 not in levels:
		levels = (0,) + tuple(levels)

	collec_poly = plt.contourf(
		x, y, z, levels, cmap=plt.cm.rainbow,
		vmax=abs(z).max(), vmin=-abs(z).max(), alpha=0.35
		)

	geoms = []
	result = []
	for multipoly, level_value in zip(collec_poly.collections, levels[1:]):  # first collection is for 0th belt
		for polygon in multipoly.get_paths():

			rings = polygon.to_polygons()

			if len(rings) == 0:
				continue

			if len(rings[0]) < 4:  # if outside ring is empty we don't create this polygon
				continue

			# internal rings can also have 0 length, drop them too
			rings[1:] = [r for r in rings[1:] if len(r) > 3]
			geoms.append((rings[0], rings[1:]))

		# by default 10-minute multipolygon does not include the 5-minute mutlipolygon,
		# we add the inner multipolygons (5 min) to the outer ones (10 min)
		# for one source, this is correct.
		mp = MultiPolygon(geoms)
		if mp.is_empty:
			continue

		result.append({
			'geometry': mp.buffer(.1).buffer(-0.1),
			level_field: level_value})

	if len(result) == 0:
		return

	return gpd.GeoDataFrame.from_dict(result)


class IsochroneRouter:
	SECONDS_PER_PIXEL = 15

	def __init__(self, origin, router, levels, speed:float):
		self.origin = origin
		import re
		if router not in CONFIG['routers'] and not re.match(r'^https?\://.*', router):
			raise ValueError('router should be a key in erde config routers section, or a URL')

		self.router = CONFIG['routers'].get(router, router)
		self.levels = tuple(levels)
		# speed should be upper limit on average moving speed in kmh.
		# average speeds are:
		# 5 kmh for pedestrian
		# 12 kmh for cyclist
		# 25 kmh for car in city (usually ~20 kmh)
		# 60 kmh for car between cities
		self.speed = speed

		self.max_snap = MAX_SNAP
		self.mts = MAX_TABLE_SIZE

		self.grid_density = 1.0  # change this to set density of routing points grid. The effect is linear (not quadratic), but size of grid changes so that the grid snaps to the bounding box on both sides, grid size changes in steps.
		self._grid_step = None  # or change this to override grid_density and set distance directly
		self._grid = None
		self._routed = None

		self._nan_value = 36000
		self._raster_size = None

	@property
	def radius(self):
		return self.speed / KMH2MPS * max(self.levels) * 60

	@property
	def raster_size(self):
		"""Size of raster grid."""
		from math import log
		return int(round(self.radius / max(log(self.radius * 2, 2), 1)))

	def get_grid_step(self):
		if self._grid_step is None:
			# we'll put routing points every 30 seconds in a hexagonal grid
			# we take square root of grid density to make it have linear effect on grid size
			self._grid_step = self.speed / KMH2MPS * GRID_EVERY_N_SECONDS / (self.grid_density ** .5)
		return self._grid_step

	def set_grid_step(self, val):
		self._grid_step = val

	grid_step = property(get_grid_step, set_grid_step)

	@property
	def bounds(self):
		return utils.transform(self.origin, 4326, 3857).buffer(self.radius / utils.coslat(self.origin), resolution=3).bounds

	def get_grid(self):
		"""
		Generates grid of points for isochrones.
		"""
		if self._grid is not None:
			return self._grid

		# we make buffer, then bounding box to clip grid points
		# when the grid was limited with a circle, it often made triangulation errors

		x1, y1, x2, y2 = self.bounds
		# adjust step to 3857 CRS scale coeff
		grid_step_local = self.grid_step / utils.coslat(self.origin)
		# make an int number of grid steps horizontally
		grid_step_local = (x2 - x1) / round((x2 - x1) / grid_step_local)
		xstep = grid_step_local * 2
		xoffset = xstep / 2
		ystep = grid_step_local * 2 * (3 ** .5)
		yoffset = ystep / 2

		x, y = np.mgrid[x1:x2 + 1:xstep, y1:y2 + 1:ystep]
		xlist = list(x.flatten())
		ylist = list(y.flatten())

		dprint(f'grid {len(xlist)}')

		# hex-grid second half, shifted by half xstep/ystep
		# but upper limits on X & Y should be the same to fit in the box
		x, y = np.mgrid[x1 + xoffset:x2 + .1:xstep, y1 + yoffset:y2 + .1:ystep]
		xlist += list(x.flatten())
		ylist += list(y.flatten())

		geoms = gpd.GeoSeries(list(map(Point, zip(xlist, ylist))))
		self._grid = gpd.GeoDataFrame({'geometry': geoms}, crs=3857).to_crs(4326)
		return self._grid

	def set_grid(self, grid):
		self._grid = grid

	grid = property(get_grid, set_grid)

	def get_routed(self):
		"""Generates table route results for the origin."""
		if self._routed is not None:
			return self._routed

		origin_gdf = gpd.GeoDataFrame({'geometry': [self.origin], FULL_DURATION: [0]}, index=[-1], crs=4326)
		result = pd.concat(table_route([self.origin], self.grid, self.router, max_table_size=self.mts, pbar=False))
		result['geometry'] = result['geometry_dest']
		result.drop(['new_geometry', 'new_geometry_dest', 'geometry_dest'], axis=1, errors='ignore', inplace=True)

		result = gpd.GeoDataFrame(result, crs=4326)
		result = result.iloc[result['duration'].to_numpy().nonzero()[0]][:]

		result[FULL_DURATION] = result.duration + (result.source_snap + result.destination_snap) / SNAP_SPEED * KMH2MPS
		result = pd.concat([result, origin_gdf])
		self._routed = result.to_crs(3857)
		return self._routed

	def set_routed(self, val):
		self._routed = val

	routed = property(get_routed, set_routed)

	@property
	def raster(self):
		"""Generates or returns the cached raster, from which the level cursev and polygons are made.
		"""
		if len(self.grid) < 4:
			return

		gdf = self.routed
		minx, miny, maxx, maxy = gdf.total_bounds

		x = gdf.geometry.x
		y = gdf.geometry.y
		z = gdf[FULL_DURATION]

		# Grid of pixels to make raster and then interpolate values from it
		xi = np.linspace(minx, maxx, self.raster_size)
		yi = np.linspace(maxy, miny, self.raster_size)
		# make all combinations of Xi and Yi (400*400), then unwrap it in one line (in 2 rows (x, y) by 160k)
		ci = np.reshape(np.meshgrid(xi, yi), (2, self.raster_size * self.raster_size))

		# triangulator that takes points and gives the values in between
		triang = Triangulation(x, y)
		# interpolated matrix
		interp = LinearTriInterpolator(triang, z)
		# interpolate values, then re-wrap it into 400 by 400 for contourf (it requires a grid like pixels)

		zi = np.reshape(interp(*ci), (self.raster_size, self.raster_size))
		zi = np.where(np.isnan(zi), self._nan_value, zi)
		return xi, yi, zi

	@property
	def polygons(self):
		if len(self.grid) < 4:
			return

		result = raster2polygons(*self.raster, [i * 60 for i in self.levels], POLY_DURATION_COL)
		result.crs = 3857
		return result.to_crs(4326)


@autocli
def main(sources: gpd.GeoDataFrame, router, durations, speed:float, grid_density:float = 1.0, max_snap: float = MAX_SNAP, mts: int = MAX_TABLE_SIZE, pbar:bool=False) -> write_stream:
	"""Builds isochrones from sources points within durations (iterable of numeric, minutes). Routes will start from the sources to a grid of points. To calculate the span and density of the grid, `speed` is required. Speed is upper limit of mean speed in km/h (see a list below). Isochrones are returned as a dataframe with same fields as in sources df, plus duration column and geometry as MultiPolygons (each isochrone may have detached islands).

	Parameters
	----------
	sources: GeoDataFrame of points
		Each geometry value becomes the source of isochrone.
	router: string
		Name of router in config, or URL, e.g. 'http://localhost:5000'.
	durations: iterable of numbers
		Time limits of isochrones for each source, in minutes, e.g. `(5, 10, 15)`
	speed: float
		Mean speed of the supposed mode of transport in km/h, e.g. 5 for walking, 25 for car in a city. See a list of values below.
	grid_density: float, default 1.0
		A variable used to increase density of points in the routing grid. Has linear effect, i.e. 2.0 makes twice more points in the grid (and sq.root of 2 less distance between them).
	max_snap: float, default 250
		Max distance in metres from the grid points to road graph to be counted as reachable.
	mts: int, default 2000
		Maximum table in one HTTP request to OSRM. See comments below.
	pbar: bool, default False
		Show progress bar in command line or Jupyter notebook.

	Returns
	-------
	gpd.GeoDataFrame with MultiPolygon geometries, and other columns as in sources geodataframe, plus durations.

	`speed` parameter
	-----------------
	Common values to use:

	* walking: 5
	* cycling: 12
	* car in city: 25
	* car outside of cities in ordinary roads: 60
	* car on autobahn/highway: 100

	The `speed` parameter is better than radius, because it remans constant regardless of max duration. No default speed or radius would fit all modes of transport. Wrong value would make cropped isochrones or too large tables and long calculations (complexity is quadratic).

	`mts` parameter
	---------------
	Eeach isochrone requires a grid of several hundred points. To build them, the script uses OSRM table-route feature (https://github.com/Project-OSRM/osrm-backend/blob/master/docs/http.md). Each table has N sources * M destinations cells, and this N*M number is limited on the server side.

	On public servers, the limit is around 2K. 100K works normally on a consumer computer and servers. If you run the router yourself, launch it with a parameter:

		osrm-routed my-data-file.osrm --max-table-size=100000

	An intermediary http server also limits URLs length. To work around it, set the limit at 2K or lower.
	"""

	# NOTE for developers/users: the number of parameters was minimized as possible, but this is necessary as these parameters often change between servers.

	# It would make sense to make `speed` unnecessary and stored in config, but no idea how to make it required if router is URL.
	pass
