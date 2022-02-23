# eo-io
Read and write to an S3 object store


## Store dataset

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



## Store Sentinel-Hub data

    storage = eo_io.ReadWriteData(config_s3)
