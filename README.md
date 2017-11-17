# cattle

`curl -i -F "csv=@/home/amp/src/cattle/data/imf.csv" http://localhost:8088/build`
`curl -F "csv=@/home/amp/src/cattle/data/imf.csv" http://localhost:8088/build > imf.csv-metadata.json`
`curl -i -F "csv=@/home/amp/src/cattle/data/imf.csv" -F "json=@imf.csv-metadata.json" http://localhost:8088/convert`
`curl -i -F "csv=@/home/amp/src/cattle/data/imf.csv" -F "json=@imf.csv-metadata.json" http://localhost:8088/convert -H'Accept: text/turtle'`
`curl -F "csv=@/home/amp/src/cattle/data/imf.csv" -F "json=@imf.csv-metadata.json" http://localhost:8088/convert -H'Accept: text/turtle' > imf.csv.ttl`
`curl -i -F "csv=@/home/amp/src/cattle/data/imf.csv" -F "json=@imf.csv-metadata.json" http://localhost:8088/convert -H'Accept: application/n-quads'`
`curl -F "csv=@/home/amp/src/cattle/data/imf.csv" -F "json=@imf.csv-metadata.json" http://localhost:8088/convert -H'Accept: application/n-quads' > imf.csv.nq`
