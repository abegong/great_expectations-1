from .base import DataAssetProfiler
from .basic_dataset_profiler import BasicDatasetProfiler

class MultiDatasetProfiler(DataAssetProfiler):
    """
    WARNING: This profiler is in ALPHA.
    Sparsely tested and documented. Not yet stable.
    We will change it in the future and not consider the changes "breaking."
    Use at your own risk.
    
    This profiler is intended to run without a DataContext.
    It has no facilities for persisting EVRs or Metrics.
    Instead, it's focused on the core logic of profiling.
    """

    def _get_all_expectation_validation_results_for_one_expectation_configuration(self):
        pass

    def _create_bootstrapped_expectations(self, initial_expectation_suite, expectation_validation_results):
        pass

    def _generate_all_expectation_validation_results(self, dataset_list):
        evrs = {}
        for batch_date, batch in batches.items():
            evrs[batch_date] = ge.validate(batch, my_exp_suite)

    def _select_representative_dataset(self, dataset_list):
        assert len(dataset_list) > 0

        return dataset_list[0]

    def _generate_initial_expectation_suite(self, dataset_list):
        representative_dataset = self._select_representative_dataset(dataset_list)

        initial_expectation_suite, _ = BasicDatasetProfiler.profile(representative_dataset)

        return initial_expectation_suite

    def profile(self, dataset_list):
        initial_expectation_suite = self._generate_initial_expectation_suite(dataset_list)
        expectation_validation_results = self._generate_all_expectation_validation_results(datasets)
