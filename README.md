# eo-io
Read and write to an S3 object store

ee Store Dataset below to write to dataset, e.g. for use with eo-io the eoain processing chain.

See Store Sentinel-Hub Data for use with the Sentinel-Hub API, e.g. for the eo-custom-scripts processing chain.

## Store Dataset

Firstly import the eo_io package

    import eo_io

Initialise the data store, with the Xarray dataset, 

    store = eo_io.store_dataset.store(dataset, name, info)

Write the data to a GeoTIFF and save the data in the object store

    store.to_tiff()

Write the metadata to a JSON file and save the data in the object store

    store.metadata_to_json()

Save the data in the Zarr format on the object store 
    
    store.to_zarr()



## Store Sentinel-Hub Data


    import eo_io

    store = eo_io.store_geotiff.ToS3(processing_module, frequency, request_func, testing)
    for prod_name in store.to_storage():
        print('s3-location: ' + ' '.join(prod_name))
        yield prod_name

