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

    evr_fields_by_expectation_type = {
        "expect_table_column_count_to_equal" : {
            "pattern" : "single_value",
            "source_field" : "observed_value",
            "target_field" : "value",
        },
        "expect_column_kl_divergence_to_be_less_than" : {"pattern": "punt"},
        "expect_column_value_lengths_to_be_between" : {"pattern": "punt"},
    #     {
    #         "pattern" : "min_and_max_values",
    #         "source_field" : "unexpected_count",
    #         "target_field" : "value",
    #     },
        "expect_table_row_count_to_be_between" : {
            "pattern" : "min_and_max_values",
            "source_field" : "observed_value",
            "target_field" : "value",
        },
        "expect_table_columns_to_match_ordered_list" : {"pattern": "punt"},
        "expect_column_values_to_be_in_type_list" : {"pattern": "punt"},
        "expect_column_unique_value_count_to_be_between" : {
            "pattern" : "min_and_max_values",
            "source_field" : "observed_value",
            "target_field" : "value",
        },
        "expect_column_proportion_of_unique_values_to_be_between" : {
            "pattern" : "min_and_max_values",
            "source_field" : "observed_value",
            "target_field" : "value",
        },
        "expect_column_values_to_be_unique" : {
            "pattern" : "single_value",
            "source_field" : "unexpected_percent",
            "target_field" : "mostly",
        },
        "expect_column_values_to_not_be_null" : {
            "pattern" : "single_value",
            "source_field" : "unexpected_percent",
            "target_field" : "mostly",
        },
        "expect_column_values_to_be_in_set" : {
            "pattern" : "single_value",
            "source_field" : "unexpected_percent",
            "target_field" : "mostly",
        },
        "expect_column_values_to_not_match_regex" : {
            "pattern" : "single_value",
            "source_field" : "unexpected_percent",
            "target_field" : "mostly",
        },
        "expect_column_min_to_be_between" : {
            "pattern" : "min_and_max_values",
            "source_field" : "observed_value",
            "target_field" : "value",
        },
        "expect_column_max_to_be_between" : {
            "pattern" : "min_and_max_values",
            "source_field" : "observed_value",
            "target_field" : "value",
        },
        "expect_column_mean_to_be_between" : {
            "pattern" : "min_and_max_values",
            "source_field" : "observed_value",
            "target_field" : "value",
        },
        "expect_column_median_to_be_between" : {
            "pattern" : "min_and_max_values",
            "source_field" : "observed_value",
            "target_field" : "value",
        },
        "expect_column_stdev_to_be_between" : {
            "pattern" : "min_and_max_values",
            "source_field" : "observed_value",
            "target_field" : "value",
        },
        "expect_column_quantile_values_to_be_between" : {"pattern": "punt"},
        "expect_column_distinct_values_to_be_in_set" : {"pattern": "punt"},
    }

    def _create_expectation_from_bootstrapped_evr_list(self, expectation_type, original_kwargs, evr_list):
        modified_kwargs = copy.deepcopy(original_kwargs)
        changes_made = False
        
        modified_kwargs = original_kwargs

        mapping_pattern_dict = self.evr_fields_by_expectation_type[expectation_type]

        if mapping_pattern_dict["pattern"] == "single_value":
            if mapping_pattern_dict["source_field"] == "observed_value":
                values = [evr.result["observed_value"] for evr in evr_list]
            elif mapping_pattern_dict["source_field"] == "unexpected_percent":
                values = [evr.result["unexpected_percent"] for evr in evr_list]
                
            target_value = pd.Series(values).mode()[0]
            
            target_field = mapping_pattern_dict["target_field"]
            if target_field == "mostly":
                target_value = 1-target_value
            
                if target_value == 1.0:
                    if "mostly" in modified_kwargs:
                        del(modified_kwargs["mostly"])
                else:
                    modified_kwargs[target_field] = target_value
            
            else:
                modified_kwargs[target_field] = target_value
            
            changes_made = True

        elif mapping_pattern_dict["pattern"] == "min_and_max_values":
            if mapping_pattern_dict["source_field"] == "observed_value":
                values = [evr.result["observed_value"] for evr in evr_list]
            elif mapping_pattern_dict["source_field"] == "unexpected_percent":
                values = [evr.result["unexpected_percent"] for evr in evr_list]

            modified_kwargs["min_value"] = pd.Series(values).min()
            modified_kwargs["max_value"] = pd.Series(values).max()
            
            changes_made = True

        elif mapping_pattern_dict["pattern"] == "punt":
            pass

        else:
            raise ValueError("Unknown pattern: " + mapping_pattern_dict["pattern"])

        if changes_made:      
            return ExpectationConfiguration(
                expectation_type=expectation_type,
                kwargs=modified_kwargs,
            )

    def _create_grouped_expectation_validation_results(self, initial_expectation_suite, expectation_suite_validation_results):
        """
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
                grouped_expectation_validation_results[(base_expectation.expectation_type, json.dumps(base_expectation.kwargs.to_json_dict()))] = grouped_results
        
        return grouped_expectation_validation_results

    def _create_bootstrapped_expectation_suite(self, initial_expectation_suite, expectation_suite_validation_results):
        grouped_evrs = self._create_grouped_expectation_validation_results(
            initial_expectation_suite,
            expectation_suite_validation_results,
        )

        modified_expectation_suite = ExpectationSuite(
            expectation_suite_name="mbatched-suite"
        )

        for (expectation_type, expectation_kwarg_string), evr_list in grouped_evrs.items():
            expectation_kwargs = json.loads(expectation_kwarg_string)
            
            new_expectation = self._create_expectation_from_bootstrapped_evr_list(
                expectation_type,
                expectation_kwargs,
                evr_list
            )

            if new_expectation != None:
                modified_expectation_suite.expectations.append(new_expectation)
        
        return modified_expectation_suite

    def _generate_all_expectation_suite_validation_results(self, initial_expectation_suite, dataset_list):
        expectation_suite_validation_results = []
        for index, dataset in enumerate(dataset_list):
            expectation_suite_validation_results.append(
                validate(dataset, initial_expectation_suite)
            )
        
        return expectation_suite_validation_results

    def _generate_initial_expectation_suite(self, dataset_list):
        """Create an initial ExpectationSuite by profiling the first dataset in the list

        Note: this method is highly sensitive to the shape of the first dataset in the list.
        """
        assert len(dataset_list) > 0
        representative_dataset = dataset_list[0]

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
        modified_expectation_suite = self._create_bootstrapped_expectation_suite(
            initial_expectation_suite,
            expectation_suite_validation_results,
        )

        return modified_expectation_suite
