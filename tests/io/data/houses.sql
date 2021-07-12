DROP TABLE IF EXISTS houses;
CREATE TABLE public.houses (
    id integer,
    hid bigint,
    city_id bigint,
    quarter_id bigint,
    addr character varying(255),
    pop integer,
    apts integer,
    year integer,
    centroid public.geometry(Point,900913),
    interpolated boolean
);
INSERT INTO public.houses VALUES (147816, 2610888, 1, 59365, 'Жемчужная ул, 2', 72, 30, 1961, '010100002031BF0D001216EB9F9FA461415C305A08FEF55B41', false);
INSERT INTO public.houses VALUES (146626, 2607689, 1, 59365, 'Весенний проезд, 4а', 139, 80, 1965, '010100002031BF0D00BE9F078064A46141BC067A1D0CF75B41', false);
INSERT INTO public.houses VALUES (146625, 2603495, 1, 59365, 'Весенний проезд, 4', 120, 64, 1961, '010100002031BF0D00BB54DF3E53A461416ACA08D8EFF65B41', false);
INSERT INTO public.houses VALUES (145557, 2611142, 1, 59365, 'Весенний проезд, 6', 105, 64, 1962, '010100002031BF0D000CD3AAC176A46141EF7ADA2A23F75B41', false);