# eo-io
Read and write to an S3 object store

eoconfig

The 

    import eo_io
    data_stores = eo_io.store_dataset.Stores()
    store = data_stores.insert_dataset(dataset, name, info)
    store.to_tiff()
    store.metadata_to_json()
    store.to_zarr()


    eo_io.store_geotiff.ToS3(storage, processing_module, frequency, request_func, testing).to_storage()