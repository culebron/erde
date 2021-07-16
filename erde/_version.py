import re
import os
import subprocess


def shell_exec(cmd: str, cwd=None) -> str:
    return subprocess.check_output(cmd, shell=True, cwd=cwd).decode('utf-8').strip()


def parse_tag(tag_text):
    match = re.match(r'^v(\d+)\.(\d+)\.(\d+)(.*$)', tag_text)
    return int(match.group(1)), int(match.group(2)), int(match.group(3)), match.group(4)


def git_is_master(path=None):
    return shell_exec('git show-ref --hash --head origin/master | uniq | wc -l', cwd=path) == '1'


def git_last_version(path=None):
    major, minor, patch, rest = parse_tag(shell_exec('git describe --tags --always', cwd=path))
    return major, minor, patch, rest


def git_next_patch(major, minor, path=None):
    tag_list = shell_exec(f'git tag --list | grep "v{major}.{minor}"', cwd=path).split('\n')
    return 1 + int(max(map(lambda t: parse_tag(t)[2], tag_list)))


def git_branch(path=None):
    return shell_exec('git rev-parse --abbrev-ref HEAD', cwd=path)


def git_version(path=None):
    major, minor, patch, rest = git_last_version(path)
    next_patch = git_next_patch(major, minor, path=path)

    if git_is_master(path):
        if rest:
            return f'{major}.{minor}.{next_patch}'
        else:
            return f'{major}.{minor}.{patch}'
    else:
        rest = rest.replace('-', '.')
        branch = git_branch(path).replace('_', '.').lower()
        return f'{major}.{minor}.{next_patch}+{branch}.{rest}'


def write_version(path):
    version = ''
    if os.path.isdir('.git'):
        version = git_version()
        open(path, 'w+').write(f'__version__ = "{version}"')
    return version
