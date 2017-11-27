# cattle

[cattle](http://cattle.datalegend.net/) is a [COW](https://github.com/CLARIAH/COW) Web service

## Example 

`curl -i -F "csv=@/home/amp/src/cattle/data/imf.csv" http://cattle.datalegend.net/build`

`curl -F "csv=@/home/amp/src/cattle/data/imf.csv" http://cattle.datalegend.net/build > imf.csv-metadata.json`

`curl -i -F "csv=@/home/amp/src/cattle/data/imf.csv" -F "json=@imf.csv-metadata.json" http://cattle.datalegend.net/convert`

`curl -i -F "csv=@/home/amp/src/cattle/data/imf.csv" -F "json=@imf.csv-metadata.json" http://cattle.datalegend.net/convert -H'Accept: text/turtle'`

`curl -F "csv=@/home/amp/src/cattle/data/imf.csv" -F "json=@imf.csv-metadata.json" http://cattle.datalegend.net/convert -H'Accept: text/turtle' > imf.csv.ttl`

`curl -i -F "csv=@/home/amp/src/cattle/data/imf.csv" -F "json=@imf.csv-metadata.json" http://cattle.datalegend.net/convert -H'Accept: application/n-quads'`

`curl -F "csv=@/home/amp/src/cattle/data/imf.csv" -F "json=@imf.csv-metadata.json" http://cattle.datalegend.net/convert -H'Accept: application/n-quads' > imf.csv.nq`
