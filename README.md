# cattle

[cattle](http://cattle.datalegend.net/) is a [COW](https://github.com/CLARIAH/COW) Web service

## Examples

To understand this service please read the [COW](https://github.com/clariah/cow) documentation first! To convert csv's to Linked Data using COW this webservice provides the following options (in steps matching the COW 'logic').

### Step 1: build a metadata json file
Build a metadata.json file containing the conversion script and save it as a file
`curl -F "csv=@/home/amp/src/cattle/data/imf.csv" http://cattle.datalegend.net/build > imf.csv-metadata.json`
WARNING!: Unlike using COW locally, this will actually OVERWRITE a previous build of your file!

### Step 2: change your metadata file
This is something you do locally, so manually edit the `*-metadata.json` file you just created.

### Step 3: convert your csv file using the metadata.json script you created
`curl -F "csv=@/home/amp/src/cattle/data/imf.csv" -F "json=@imf.csv-metadata.json" http://cattle.datalegend.net/convert -H'Accept: application/n-quads' > imf.csv.nq`


### Other examples
If you just want to print something on your screen and not write them, simply omit the `> ...` part. E.g.:
`curl -i -F "csv=@/home/amp/src/cattle/data/imf.csv" http://cattle.datalegend.net/build`
`curl -i -F "csv=@/home/amp/src/cattle/data/imf.csv" -F "json=@imf.csv-metadata.json" http://cattle.datalegend.net/convert`
`curl -i -F "csv=@/home/amp/src/cattle/data/imf.csv" -F "json=@imf.csv-metadata.json" http://cattle.datalegend.net/convert -H'Accept: application/n-quads'`

Please note, that the webservice also allows you to save Linked Data as turtle (contrary to COW):
`curl -i -F "csv=@/home/amp/src/cattle/data/imf.csv" -F "json=@imf.csv-metadata.json" http://cattle.datalegend.net/convert -H'Accept: text/turtle'`
`curl -F "csv=@/home/amp/src/cattle/data/imf.csv" -F "json=@imf.csv-metadata.json" http://cattle.datalegend.net/convert -H'Accept: text/turtle' > imf.csv.ttl`



