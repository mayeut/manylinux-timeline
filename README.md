# manylinux-timeline

## Policy for dropping python versions

- Python versions will be kept at least 2 years after EOL
- Python versions will be dropped 4 years after EOL at the latest
- Between those two dates, versions will be dropped if their overall download proportion at the beginning of the graph is less than 3%

This can be summed-up as:

|             | 2026-10 | 2027-06 | 2027-10 | 2028-10 | 2029-10 | 2030-10 | 2031-10 |
|-------------|---------|---------|---------|---------|---------|---------|---------|
| Python 3.7  | 🧟      | ❌       |         |         |         |         |         |
| Python 3.8  | 🧟      | 🧟      | 🧟      | ❌       |         |         |         |
| Python 3.9  | 💀      | 💀      | 🧟      | 🧟      | ❌       |         |         |
| Python 3.10 | 💀      | 💀      | 💀      | 🧟      | 🧟      | ❌       |         |
| Python 3.11 | ✅       | ✅       | 💀      | 💀      | 🧟      | 🧟      | ❌       |
| Python 3.12 | ✅       | ✅       | ✅       | 💀      | 💀      | 🧟      | 🧟      |
| Python 3.13 | ✅       | ✅       | ✅       | ✅       | 💀      | 💀      | 🧟      |
| Python 3.14 | ✅       | ✅       | ✅       | ✅       | ✅       | 💀      | 💀      |
| Python 3.15 | 🚀      | ✅       | ✅       | ✅       | ✅       | ✅       | 💀      |
| Python 3.16 | 🔨      | 🔨      | 🚀      | ✅       | ✅       | ✅       | ✅       |
| Python 3.17 |         |         | 🔨      | 🚀      | ✅       | ✅       | ✅       |
| Python 3.18 |         |         |         | 🔨      | 🚀      | ✅       | ✅       |
| Python 3.19 |         |         |         |         | 🔨      | 🚀      | ✅       |

Legend:
* ❌: Drop unconditionally 4 years after EOL
* 🧟: EOL versions that are still shown if their overall download proportion at the beginning of the graph is more than 3%
* 💀: EOL versions that are still shown unconditionally
* ✅: non-EOL python versions
* 🚀: release of new python version
* 🔨: pre-release shown in preview

## Thanks

This is derivative work from [Drop Python](https://hugovk.github.io/drop-python/), a site that tracks progress of packages dropping old (EOL) python versions.
Thanks also to [Python Wheels](https://pythonwheels.com), a site that tracks progress in the new Python package distribution standard called [Wheels](https://pypi.org/project/wheel), [Python 3 Wall of Superpowers](https://python3wos.appspot.com/) for the concept and making their code open source, and see also [Python 3 Readiness](http://py3readiness.org).
