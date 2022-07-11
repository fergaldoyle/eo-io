# eo-io
Read and write to an S3 object store

See Store Dataset below to write to dataset, e.g. for use with eo-io the eoain processing chain.

See Store Sentinel-Hub Data below for use with the Sentinel-Hub API, e.g. for the eo-custom-scripts processing chain.


See [eo-custom-scripts](https://github.com/ECHOESProj/eo-custom-scripts) 
and [eoain](https://github.com/ECHOESProj/eoian) for example of eo-io use. 

### Credentials

The credential can be obtained from the Compass Informatics password manager, under "eo-custom-scripts configuration files".
Unzip the config files in there and put the yaml files in the home directory in a directory called eoconfig.

## Store Dataset

Firstly import the eo_io package

    import eo_io

Initialise the data store, with the Xarray dataset, 

    store = eo_io.store_dataset.store(dataset, name, info)
where dataset is and xarray dataset, name is the name of the processing chain and info is a dictionary containing 
information about source products (from EODag).

Write the data to a GeoTIFF and save the data in the object store

    store.to_tiff()

Write the metadata to a JSON file and save the data in the object store

    store.metadata_to_json()

Save the data in the Zarr format on the object store 
    
    store.to_zarr()



## Store Sentinel-Hub Data


    import eo_io

    s3 = eo_io.store_geotiff.ToS3(processing_module, frequency, request_func, testing)
where processing_module is the name of the custom scripts processing module,
      frequency is frequency of the output product daily, monthly, yearly...
      request_func is the Sentinel-Hub function is an instance of SentinelHubRequest
      testing is a boolean: True if the function is used for testing