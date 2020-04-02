import json

import pandas as pd

import great_expectations as ge
from great_expectations.data_context.util import file_relative_path
from great_expectations.data_context.util import safe_mmkdir
from great_expectations.profile.multidataset_profiler import MultiDatasetProfiler
from great_expectations.core import (
    ExpectationSuite,
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
    ExpectationConfiguration,
    ExpectationKwargs,
)

#TODO: turn this into a proper fixture
test_datasets = [
    ge.from_pandas(pd.DataFrame({
        "x" : [0,1,2,3,4,5],
        "y" : list("abcedf"),
        "z" : list("yynnny"),
    })),
    ge.from_pandas(pd.DataFrame({
        "x" : [0,1,2,3,4,5],
        "y" : list("abcedf"),
        "z" : list("yynnny"),
    })),
    ge.from_pandas(pd.DataFrame({
        "x" : [0,1,2,3,4,5],
        "y" : list("abcedf"),
        "z" : list("yynnny"),
    })),
    ge.from_pandas(pd.DataFrame({
        "x" : [0,1,2,3,4,5,6],
        "y" : list("abcedfg"),
        "z" : list("yynNNyn"),
    })),
    ge.from_pandas(pd.DataFrame({
        "x" : [0,1,2,3,4,None,None],
        "y" : list("abcedfg"),
        "z" : list("yynNNyn"),
    })),
    # ge.from_pandas(pd.DataFrame({
    #     "x" : [0,1,2,3,4,5],
    #     "y" : list("abcedf"),
    # })),
    ge.from_pandas(pd.DataFrame({
        "x" : [-20,1,2,3,4,5,6],
        "y" : list("abcedfg"),
        "z" : list("nnnnnnn"),
    })),
]

def test_smoke_MultiDatasetProfiler(test_datasets=test_datasets):
    """This smoke test is a placeholder while MultiDatasetProfiler is in alpha.

    It will eventually be replaced with true unit tests, and the smoke test will be moved into an intergration test framework.
    In the meantime, it illustrates usage and verifies that none of MultiDatasetProfiler's dependencies have broken.
    """

    my_profiler = MultiDatasetProfiler()
    expectation_suite = my_profiler.profile(test_datasets)

    assert isinstance(expectation_suite, ExpectationSuite)
    assert len(expectation_suite.expectations) > 0
    expectation_types = set([expectation.expectation_type for expectation in expectation_suite.expectations])
    # print(expectation_types)
    for expectation_type in [
        'expect_column_proportion_of_unique_values_to_be_between',
        'expect_table_row_count_to_be_between',
        'expect_column_values_to_not_be_null',
        'expect_column_values_to_be_in_set',
        'expect_column_values_to_not_match_regex',
        'expect_column_unique_value_count_to_be_between',
        'expect_column_values_to_be_unique',
    ]:
        assert expectation_type in expectation_types

    # ??? : Is this the right place for this line:
    safe_mmkdir(file_relative_path(__file__, './output'))
    with open(file_relative_path(__file__, './output/test_smoke_MultiDatasetProfiler.json'), 'wb') as f:
        f.write(json.dumps(expectation_suite.to_json_dict()).encode("utf-8"))

    from great_expectations.render.renderer import ExpectationSuitePageRenderer
    from great_expectations.render.view import DefaultJinjaPageView

    espr = ExpectationSuitePageRenderer()
    rdc = espr.render(expectations=expectation_suite)
    html = DefaultJinjaPageView().render(rdc)

    with open(file_relative_path(__file__, './output/test_smoke_MultiDatasetProfiler.html'), 'wb') as f:
        f.write(html.encode("utf-8"))

    # print(expectation_suite)
    # assert False


def test_MultiDatasetProfiler_column_whitelist(test_datasets=test_datasets):
    my_profiler = MultiDatasetProfiler(
        column_whitelist=["x"]
    )
    expectation_suite = my_profiler.profile(test_datasets)

    for exp in expectation_suite.expectations:
        if "column" in exp["kwargs"]:
            assert exp["kwargs"]["column"] == "x"


def test_MultiDatasetProfiler_expectation_whitelist(test_datasets=test_datasets):
    my_profiler = MultiDatasetProfiler(
        expectation_whitelist=[
            'expect_column_values_to_not_be_null',
            'expect_column_values_to_not_match_regex',
        ]
    )
    expectation_suite = my_profiler.profile(test_datasets)
    for exp in expectation_suite.expectations:
        assert exp["expectation_type"] in [
            'expect_column_values_to_not_be_null',
            'expect_column_values_to_not_match_regex',
        ]

def test_MultiDatasetProfiler_expectation_blacklist(test_datasets=test_datasets):
    my_profiler = MultiDatasetProfiler(
        expectation_blacklist=[
            'expect_column_values_to_not_be_null',
        ]
    )
    expectation_suite = my_profiler.profile(test_datasets)
    expectation_types = set([exp["expectation_type"] for exp in expectation_suite.expectations])

    #This is kind of a weak test.
    assert "expect_column_values_to_not_be_null" not in expectation_types


def test_MultiDatasetProfiler_validation_result_store(test_datasets=test_datasets):
    #FIXME: This doesn't use a real store. It uses a list.
    my_fake_store = []

    assert len(my_fake_store) == 0
    my_profiler = MultiDatasetProfiler(
        validation_result_store=my_fake_store
    )
    _ = my_profiler.profile(test_datasets)

    assert len(my_fake_store) > 0

