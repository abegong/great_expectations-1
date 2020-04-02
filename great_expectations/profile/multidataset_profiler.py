import json
import copy

import pandas as pd

from .. import validate
from .base import DataAssetProfiler
from .basic_dataset_profiler import BasicDatasetProfiler
from ..core import (
    ExpectationSuite,
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
    ExpectationConfiguration,
    ExpectationKwargs,
)

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

    # This dictionary defines profiling behavior for each expectation_type
    # NOTE: many "skip_defining_expectation_kwargs"s in here.
    # NOTE: Not all expectations are covered here.
    evr_fields_by_expectation_type = {
        "expect_table_column_count_to_equal" : {
            "method" : "define_expectation_kwargs_for_a_single_value",
            "kwargs" : {
                "source_field" : "observed_value",
                "target_field" : "value",
            }
        },
        "expect_column_kl_divergence_to_be_less_than" : {"method": "skip_defining_expectation_kwargs", "kwargs": {}},
        "expect_column_value_lengths_to_be_between" : {"method": "skip_defining_expectation_kwargs", "kwargs": {}},
        "expect_table_row_count_to_be_between" : {
            "method" : "define_expectation_kwargs_for_min_and_max_values",
            "kwargs" : {
                "source_field" : "observed_value",
            }
        },
        "expect_table_columns_to_match_ordered_list" : {"method": "skip_defining_expectation_kwargs", "kwargs": {}},
        "expect_column_values_to_be_in_type_list" : {"method": "skip_defining_expectation_kwargs", "kwargs": {}},
        "expect_column_unique_value_count_to_be_between" : {
            "method" : "define_expectation_kwargs_for_min_and_max_values",
            "kwargs" : {
                "source_field" : "observed_value",
            }
        },
        "expect_column_proportion_of_unique_values_to_be_between" : {
            "method" : "define_expectation_kwargs_for_min_and_max_values",
            "kwargs" : {
                "source_field" : "observed_value",
            }
        },
        "expect_column_values_to_be_unique" : {
            "method" : "define_expectation_kwargs_for_a_single_value",
            "kwargs" : {
                "source_field" : "unexpected_percent",
                "target_field" : "mostly",
            }
        },
        "expect_column_values_to_not_be_null" : {
            "method" : "define_expectation_kwargs_for_a_single_value",
            "kwargs" : {
                "source_field" : "unexpected_percent",
                "target_field" : "mostly",
            }
        },
        "expect_column_values_to_be_in_set" : {
            "method" : "define_expectation_kwargs_for_a_single_value",
            "kwargs" : {
                "source_field" : "unexpected_percent",
                "target_field" : "mostly",
            }
        },
        "expect_column_values_to_not_match_regex" : {
            "method" : "define_expectation_kwargs_for_a_single_value",
            "kwargs" : {
                "source_field" : "unexpected_percent",
                "target_field" : "mostly",
            }
        },
        "expect_column_min_to_be_between" : {
            "method" : "define_expectation_kwargs_for_min_and_max_values",
            "kwargs" : {
                "source_field" : "observed_value",
            }
        },
        "expect_column_max_to_be_between" : {
            "method" : "define_expectation_kwargs_for_min_and_max_values",
            "kwargs" : {
                "source_field" : "observed_value",
            }
        },
        "expect_column_mean_to_be_between" : {
            "method" : "define_expectation_kwargs_for_min_and_max_values",
            "kwargs" : {
                "source_field" : "observed_value",
            }
        },
        "expect_column_median_to_be_between" : {
            "method" : "define_expectation_kwargs_for_min_and_max_values",
            "kwargs" : {
                "source_field" : "observed_value",
            }
        },
        "expect_column_stdev_to_be_between" : {
            "method" : "define_expectation_kwargs_for_min_and_max_values",
            "kwargs" : {
                "source_field" : "observed_value",
            }
        },
        "expect_column_quantile_values_to_be_between" : {"method": "skip_defining_expectation_kwargs", "kwargs": {}},
        "expect_column_distinct_values_to_be_in_set" : {"method": "skip_defining_expectation_kwargs", "kwargs": {}},
    }

    def __init__(
        self,
        column_whitelist=None,
        expectation_whitelist=None,
        expectation_blacklist=None,
        validation_result_store=None,
    ):
        self.column_whitelist = column_whitelist
        self.expectation_whitelist = expectation_whitelist
        self.expectation_blacklist = expectation_blacklist
        self.validation_result_store = validation_result_store

    ### Methods for extracting values from an expectation_validation_result_list ###

    def skip_defining_expectation_kwargs(self, expectation_validation_result_list, original_expectation_kwargs):
        pass

    def define_expectation_kwargs_for_a_single_value(self,
        expectation_validation_result_list,
        original_expectation_kwargs,
        source_field,
        target_field,
        method="mean"
    ):
        modified_expectation_kwargs = copy.deepcopy(original_expectation_kwargs)

        value_series = pd.Series([self._extract_field_from_expectation_validation_result(evr, source_field) for evr in expectation_validation_result_list])

        if method == "mode":
            target_value = value_series.mode()[0]
        elif method == "mean":
            target_value = value_series.mean()
        else:
            raise ValueError("Unknown method: "+method)
        
        if target_field == "mostly":
            if source_field == "unexpected_percent":
                target_value /= 100.
            target_value = 1-target_value#*1./100.
        
            if target_value == 1.0:
                if "mostly" in modified_expectation_kwargs:
                    del(modified_expectation_kwargs["mostly"])
            else:
                modified_expectation_kwargs[target_field] = target_value
        
        else:
            modified_expectation_kwargs[target_field] = target_value

        return modified_expectation_kwargs

    def define_expectation_kwargs_for_min_and_max_values(self, expectation_validation_result_list, original_expectation_kwargs, source_field, method="min_and_max"):
        modified_expectation_kwargs = copy.deepcopy(original_expectation_kwargs)
        value_series = pd.Series([self._extract_field_from_expectation_validation_result(evr, source_field) for evr in expectation_validation_result_list])

        if method == "min_and_max":
            modified_expectation_kwargs["min_value"] = value_series.min()
            modified_expectation_kwargs["max_value"] = value_series.max()
        else:
            raise ValueError("Unknown method: "+method)
        
        return modified_expectation_kwargs

    ### Core methods ###

    def _extract_field_from_expectation_validation_result(self, expectation_validation_result, source_field):
        # NOTE: Abe 2020/03/07 : Eventually, it may become necessary to make allow fetching of nested fields. For now, nope.
        return expectation_validation_result.result[source_field]

    def _create_expectation_from_grouped_expectation_validation_result_list(self, expectation_type, original_expectation_kwargs, expectation_validation_result_list):
        """
        This is where the main logic for multi-batch profiling lives.
        """
        
        mapping_pattern_dict = self.evr_fields_by_expectation_type[expectation_type]
        generation_method = getattr(self, mapping_pattern_dict["method"])
        modified_expectation_kwargs = generation_method(
            expectation_validation_result_list,
            original_expectation_kwargs,
            **mapping_pattern_dict["kwargs"]
        )

        if modified_expectation_kwargs != None:      
            return ExpectationConfiguration(
                expectation_type=expectation_type,
                kwargs=modified_expectation_kwargs,
            )

    def _create_grouped_expectation_validation_results(self, initial_expectation_suite, expectation_suite_validation_results):
        """Create a grouped_expectation_validation_results object.

        Note: This method is very ad hoc.
        It should (eventually) be made compatible with MetricStores.
        
        Whether or not to use an actual MetricStore is an open question,
        since this class isn't supposed to depend on DataContext and
        MetricStores are a DataContext concern
        """

        grouped_expectation_validation_results = {}

        for base_expectation in initial_expectation_suite.expectations:
            grouped_results = []

            for expectation_suite_validation_result in expectation_suite_validation_results:
                counter = 0
                
                for expectation_validation_result in expectation_suite_validation_result.results:
                    if base_expectation == expectation_validation_result.expectation_config:
                        counter += 1
                        grouped_results.append(expectation_validation_result)

                assert counter == 1 # Because each ExpectationConfiguration should match exactly once in each ExpectationSuiteValidationResult

                # TODO: This data structure is super awkward. Replace with something better---probably a MetricStore or proto-MetricStore
                grouped_expectation_validation_results[(base_expectation.expectation_type, json.dumps(base_expectation.kwargs.to_json_dict()))] = grouped_results
        
        return grouped_expectation_validation_results

    def _create_modified_expectation_suite(self, initial_expectation_suite, expectation_suite_validation_results):
        """Create an ExpectationSuite that contains real guesses for ExpectationKwargs.
        """

        grouped_evrs = self._create_grouped_expectation_validation_results(
            initial_expectation_suite,
            expectation_suite_validation_results,
        )

        # TODO: make this name configurable
        modified_expectation_suite = ExpectationSuite(
            expectation_suite_name="mbatched-suite"
        )

        # TODO: This data structure is super awkward. Replace with something better---probably a MetricStore or proto-MetricStore
        for (expectation_type, expectation_kwarg_string), evr_list in grouped_evrs.items():
            expectation_kwargs = json.loads(expectation_kwarg_string)

            if self.column_whitelist != None:
                if "column" in expectation_kwargs and expectation_kwargs["column"] not in self.column_whitelist:
                    continue
            
            if self.expectation_whitelist != None:
                if expectation_type not in self.expectation_whitelist:
                    continue

            if self.expectation_blacklist != None:
                if expectation_type in self.expectation_blacklist:
                    continue

            new_expectation = self._create_expectation_from_grouped_expectation_validation_result_list(
                expectation_type,
                expectation_kwargs,
                evr_list
            )

            if new_expectation != None:
                modified_expectation_suite.expectations.append(new_expectation)
        
        return modified_expectation_suite

    def _generate_all_expectation_suite_validation_results(self, initial_expectation_suite, dataset_list):
        """Loop over datasets, validate, and store the results in an ordered list
        """

        expectation_suite_validation_results = []

        for index, dataset in enumerate(dataset_list):
            validation_result = validate(dataset, initial_expectation_suite)

            expectation_suite_validation_results.append(validation_result)

            if self.validation_result_store != None:
                self.validation_result_store.append(validation_result)
        
        return expectation_suite_validation_results

    def _generate_initial_expectation_suite(self, dataset_list):
        """Create an initial ExpectationSuite by profiling the first dataset in the list

        Note: this method is highly sensitive to the shape of the first dataset in the list.
        """
        assert len(dataset_list) > 0
        representative_dataset = dataset_list[0]

        #TODO: make the base profiler configurable.
        #TODO: add a create_vacuously_true_expectations parameter to BasicDatasetProfiler.profile
        # For this call, we'd want to use False.
        initial_expectation_suite, _ = BasicDatasetProfiler.profile(representative_dataset)
        return initial_expectation_suite

    def profile(self, dataset_list):
        """Profile a list of datasets"""

        initial_expectation_suite = self._generate_initial_expectation_suite(
            dataset_list
        )
        expectation_suite_validation_results = self._generate_all_expectation_suite_validation_results(
            initial_expectation_suite,
            dataset_list
        )
        modified_expectation_suite = self._create_modified_expectation_suite(
            initial_expectation_suite,
            expectation_suite_validation_results,
        )

        return modified_expectation_suite
