# toronto-shelters
![](https://img.shields.io/pypi/pyversions/3.svg?style=for-the-badge)
a python module to download and store yearly historical data about Shelters in Toronto via City of Toronto open data catalog.

## Installation
```Python3
pip3 install -r requirements.txt
```

## Usage Examples

**Importing Historical Data**

```python3
import_occupancy(file)
```

**Fetch Yesterday Occupancy Data**

```Python3
yesterday_occupancy()
```

**Update List of Shelters**

```Python3
update_shelters(file)
```
* *todo: build in url option to update shelter list with latest data.*
