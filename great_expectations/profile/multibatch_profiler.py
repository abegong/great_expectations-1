class MultiBatchProfiler(DataAssetProfiler):
    """
    
    This profiler requires a DataContext, which is used to simplify fetching of batches.

    Most of the actual profiling logic lives in a MultiDatasetProfiler. 
    """

    def profile(self, datasource, batch_kwarg_list=None):
        pass