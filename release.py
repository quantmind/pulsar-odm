"""Use git-agile to release

https://github.com/quantmind/git-agile
"""
from agile import AgileManager

app_module = 'odm'
note_file = 'docs/notes.md'
docs_bucket = 'quantmind-docs'

if __name__ == '__main__':
    AgileManager(config='release.py').start()
