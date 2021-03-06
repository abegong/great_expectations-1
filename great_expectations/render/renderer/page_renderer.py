import logging
import os

from six import string_types

from collections import OrderedDict

from ...core.id_dict import BatchKwargs
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.render.util import num_to_str
from .renderer import Renderer
from ..types import (
    RenderedDocumentContent,
    RenderedSectionContent,
    RenderedHeaderContent,
    RenderedTableContent,
    TextContent,
    RenderedStringTemplateContent,
    RenderedMarkdownContent,
    CollapseContent
)
from great_expectations.exceptions import ClassInstantiationError

logger = logging.getLogger(__name__)


class ValidationResultsPageRenderer(Renderer):

    def __init__(self, column_section_renderer=None):
        if column_section_renderer is None:
            column_section_renderer = {
                "class_name": "ValidationResultsColumnSectionRenderer"
            }
        module_name = 'great_expectations.render.renderer.column_section_renderer'
        self._column_section_renderer = instantiate_class_from_config(
            config=column_section_renderer,
            runtime_environment={},
            config_defaults={
                "module_name": column_section_renderer.get("module_name", module_name)
            }
        )
        if not self._column_section_renderer:
            raise ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=column_section_renderer['class_name']
            )

    def render(self, validation_results):
        run_id = validation_results.meta['run_id']
        batch_id = BatchKwargs(validation_results.meta['batch_kwargs']).to_id()
        expectation_suite_name = validation_results.meta['expectation_suite_name']
        batch_kwargs = validation_results.meta.get("batch_kwargs")

        # add datasource key to batch_kwargs if missing
        if 'datasource' not in validation_results.meta.get("batch_kwargs", {}):
            # check if expectation_suite_name follows datasource.generator.data_asset_name.suite_name pattern
            if len(expectation_suite_name.split('.')) == 4:
                batch_kwargs['datasource'] = expectation_suite_name.split('.')[0]

        # Group EVRs by column
        columns = {}
        for evr in validation_results.results:
            if "column" in evr.expectation_config.kwargs:
                column = evr.expectation_config.kwargs["column"]
            else:
                column = "Table-Level Expectations"

            if column not in columns:
                columns[column] = []
            columns[column].append(evr)

        ordered_columns = Renderer._get_column_list_from_evrs(validation_results)

        overview_content_blocks = [
            self._render_validation_header(validation_results),
            self._render_validation_statistics(validation_results=validation_results),
        ]

        collapse_content_blocks = [self._render_validation_info(validation_results=validation_results)]

        if validation_results["meta"].get("batch_markers"):
            collapse_content_blocks.append(
                self._render_nested_table_from_dict(
                    input_dict=validation_results["meta"].get("batch_markers"),
                    header="Batch Markers"
                )
            )

        if validation_results["meta"].get("batch_kwargs"):
            collapse_content_blocks.append(
                self._render_nested_table_from_dict(
                    input_dict=validation_results["meta"].get("batch_kwargs"),
                    header="Batch Kwargs"
                )
            )

        if validation_results["meta"].get("batch_parameters"):
            collapse_content_blocks.append(
                self._render_nested_table_from_dict(
                    input_dict=validation_results["meta"].get("batch_parameters"),
                    header="Batch Parameters"
                )
            )

        collapse_content_block = CollapseContent(**{
            "collapse_toggle_link": "Show more info...",
            "collapse": collapse_content_blocks,
            "styling": {
                "body": {
                    "classes": ["card", "card-body"]
                },
                "classes": ["col-12", "p-1"]
            }
        })

        overview_content_blocks.append(collapse_content_block)

        sections = [
            RenderedSectionContent(**{
                "section_name": "Overview",
                "content_blocks": overview_content_blocks
            })
        ]

        if "Table-Level Expectations" in columns:
            sections += [
                self._column_section_renderer.render(
                    validation_results=columns["Table-Level Expectations"]
                )
            ]

        sections += [
            self._column_section_renderer.render(
                validation_results=columns[column],
            ) for column in ordered_columns
        ]

        return RenderedDocumentContent(**{
            "renderer_type": "ValidationResultsPageRenderer",
            "page_title": expectation_suite_name + " / " + run_id + " / " + batch_id,
            "batch_kwargs": batch_kwargs,
            "expectation_suite_name": expectation_suite_name,
            "sections": sections,
            "utm_medium": "validation-results-page",
        })

    @classmethod
    def _render_validation_header(cls, validation_results):
        success = validation_results.success
        expectation_suite_name = validation_results.meta['expectation_suite_name']
        expectation_suite_path_components = ['..' for _ in range(len(expectation_suite_name.split('.')) + 2)] \
            + ["expectations"] + expectation_suite_name.split(".")
        expectation_suite_path = os.path.join(*expectation_suite_path_components) + ".html"
        if success:
            success = '<i class="fas fa-check-circle text-success" aria-hidden="true"></i> Succeeded'
        else:
            success = '<i class="fas fa-times text-danger" aria-hidden="true"></i> Failed'
        return RenderedHeaderContent(**{
            "content_block_type": "header",
            "header": RenderedStringTemplateContent(**{
                "content_block_type": "string_template",
                "string_template": {
                    "template": 'Overview',
                    "tag": "h5",
                    "styling": {
                        "classes": ["m-0"]
                    }
                }
            }),
            "subheader": RenderedStringTemplateContent(**{
                "content_block_type": "string_template",
                "string_template": {
                    "template": "${suite_title} ${expectation_suite_name}\n${status_title} ${success}",
                    "params": {
                        "suite_title": "Expectation Suite:",
                        "status_title": "Status:",
                        "expectation_suite_name": expectation_suite_name,
                        "success": success
                    },
                    "styling": {
                        "params": {
                            "suite_title": {
                                "classes": ["h6"]
                            },
                            "status_title": {
                                "classes": ["h6"]
                            },
                            "expectation_suite_name": {
                                "tag": "a",
                                "attributes": {
                                    "href": expectation_suite_path
                                }
                            }
                        },
                        "classes": ["mb-0", "mt-1"]
                    }
                }
            }),
            "styling": {
                "classes": ["col-12", "p-0"],
                "header": {
                    "classes": ["alert", "alert-secondary"]
                }
            }
        })

    @classmethod
    def _render_validation_info(cls, validation_results):
        run_id = validation_results.meta['run_id']
        ge_version = validation_results.meta["great_expectations.__version__"]

        return RenderedTableContent(**{
            "content_block_type": "table",
            "header": RenderedStringTemplateContent(**{
                "content_block_type": "string_template",
                "string_template": {
                    "template": 'Info',
                    "tag": "h6",
                    "styling": {
                        "classes": ["m-0"]
                    }
                }
            }),
            "table": [
                ["Great Expectations Version", ge_version],
                ["Run ID", run_id]
            ],
            "styling": {
                "classes": ["col-12", "table-responsive", "mt-1"],
                "body": {
                    "classes": ["table", "table-sm"]
                }
            },
        })

    @classmethod
    def _render_nested_table_from_dict(cls, input_dict, header=None, sub_table=False):
        table_rows = []

        for kwarg, value in input_dict.items():
            if not isinstance(value, (dict, OrderedDict)):
                table_row = [
                    RenderedStringTemplateContent(**{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "$value",
                            "params": {
                                "value": str(kwarg)
                            },
                            "styling": {
                                "default": {
                                    "styles": {
                                        "word-break": "break-all"
                                    }
                                },
                            }
                        },
                        "styling": {
                            "parent": {
                                "classes": ["pr-3"],
                            }
                        }
                    }),
                    RenderedStringTemplateContent(**{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "$value",
                            "params": {
                                "value": str(value)
                            },
                            "styling": {
                                "default": {
                                    "styles": {
                                        "word-break": "break-all"
                                    }
                                },
                            }
                        },
                        "styling": {
                            "parent": {
                                "classes": [],
                            }
                        }
                    })
                ]
            else:
                table_row = [
                    RenderedStringTemplateContent(**{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "$value",
                            "params": {
                                "value": str(kwarg)
                            },
                            "styling": {
                                "default": {
                                    "styles": {
                                        "word-break": "break-all"
                                    }
                                },
                            }
                        },
                        "styling": {
                            "parent": {
                                "classes": ["pr-3"],
                            }
                        }
                    }),
                    cls._render_nested_table_from_dict(value, sub_table=True)
                ]
            table_rows.append(table_row)

        table_rows.sort(key=lambda row: row[0].string_template["params"]["value"])

        if sub_table:
            return RenderedTableContent(**{
                "content_block_type": "table",
                "table": table_rows,
                "styling": {
                    "classes": ["col-6", "table-responsive"],
                    "body": {
                        "classes": ["table", "table-sm", "m-0"]
                    },
                    "parent": {
                        "classes": ["pt-0", "pl-0", "border-top-0"]
                    }
                },
            })
        else:
            return RenderedTableContent(**{
                "content_block_type": "table",
                "header": RenderedStringTemplateContent(**{
                "content_block_type": "string_template",
                "string_template": {
                    "template": header,
                    "tag": "h6",
                    "styling": {
                        "classes": ["m-0"]
                    }
                }
            }),
                "table": table_rows,
                "styling": {
                    "classes": ["col-6", "table-responsive", "mt-1"],
                    "body": {
                        "classes": ["table", "table-sm"]
                    }
                },
            })

    @classmethod
    def _render_validation_statistics(cls, validation_results):
        statistics = validation_results["statistics"]
        statistics_dict = OrderedDict([
            ("evaluated_expectations", "Evaluated Expectations"),
            ("successful_expectations", "Successful Expectations"),
            ("unsuccessful_expectations", "Unsuccessful Expectations"),
            ("success_percent", "Success Percent")
        ])
        table_rows = []
        for key, value in statistics_dict.items():
            if statistics.get(key) is not None:
                if key == "success_percent":
                    # table_rows.append([value, "{0:.2f}%".format(statistics[key])])
                    table_rows.append([value, num_to_str(statistics[key], precision=4) + "%"])
                else:
                    table_rows.append([value, statistics[key]])

        return RenderedTableContent(**{
            "content_block_type": "table",
            "header": RenderedStringTemplateContent(**{
                "content_block_type": "string_template",
                "string_template": {
                    "template": 'Statistics',
                    "tag": "h6",
                    "styling": {
                        "classes": ["m-0"]
                    }
                }
            }),
            "table": table_rows,
            "styling": {
                "classes": ["col-6", "table-responsive", "mt-1", "p-1"],
                "body": {
                    "classes": ["table", "table-sm"]
                }
            },
        })


class ExpectationSuitePageRenderer(Renderer):

    def __init__(self, column_section_renderer=None):
        if column_section_renderer is None:
            column_section_renderer = {
                "class_name": "ExpectationSuiteColumnSectionRenderer"
            }
        module_name = 'great_expectations.render.renderer.column_section_renderer'
        self._column_section_renderer = instantiate_class_from_config(
            config=column_section_renderer,
            runtime_environment={},
            config_defaults={
                "module_name": column_section_renderer.get("module_name", module_name)
            }
        )
        if not self._column_section_renderer:
            raise ClassInstantiationError(
                module_name=column_section_renderer,
                package_name=None,
                class_name=column_section_renderer['class_name']
            )

    def render(self, expectations):
        columns, ordered_columns = self._group_and_order_expectations_by_column(expectations)
        expectation_suite_name = expectations.expectation_suite_name

        overview_content_blocks = [
            self._render_expectation_suite_header(),
            self._render_expectation_suite_info(expectations)
        ]

        table_level_expectations_content_block = self._render_table_level_expectations(columns)
        if table_level_expectations_content_block is not None:
            overview_content_blocks.append(table_level_expectations_content_block)

        asset_notes_content_block = self._render_expectation_suite_notes(expectations)
        if asset_notes_content_block is not None:
            overview_content_blocks.append(asset_notes_content_block)

        sections = [
            RenderedSectionContent(**{
                "section_name": "Overview",
                "content_blocks": overview_content_blocks,
            })
        ]

        sections += [
            self._column_section_renderer.render(expectations=columns[column]) for column in ordered_columns if column != "_nocolumn"
        ]
        return RenderedDocumentContent(**{
            "renderer_type": "ExpectationSuitePageRenderer",
            "page_title": expectation_suite_name,
            "expectation_suite_name": expectation_suite_name,
            "utm_medium": "expectation-suite-page",
            "sections": sections
        })

    def _render_table_level_expectations(self, columns):
        table_level_expectations = columns.get("_nocolumn")
        if not table_level_expectations:
            return None
        else:
            expectation_bullet_list = self._column_section_renderer.render(
                expectations=table_level_expectations).content_blocks[1]
            expectation_bullet_list.header = RenderedStringTemplateContent(**{
                "content_block_type": "string_template",
                "string_template": {
                    "template": 'Table-Level Expectations',
                    "tag": "h6",
                    "styling": {
                        "classes": ["m-0"]
                    }
                }
            })
            return expectation_bullet_list

    @classmethod
    def _render_expectation_suite_header(cls):
        return RenderedHeaderContent(**{
            "content_block_type": "header",
            "header": RenderedStringTemplateContent(**{
                "content_block_type": "string_template",
                "string_template": {
                    "template": 'Overview',
                    "tag": "h5",
                    "styling": {
                        "classes": ["m-0"]
                    }
                }
            }),
            "styling": {
                "classes": ["col-12"],
                "header": {
                    "classes": ["alert", "alert-secondary"]
                }
            }
        })

    @classmethod
    def _render_expectation_suite_info(cls, expectations):
        expectation_suite_name = expectations.expectation_suite_name
        ge_version = expectations.meta["great_expectations.__version__"]

        return RenderedTableContent(**{
            "content_block_type": "table",
            "header": RenderedStringTemplateContent(**{
                "content_block_type": "string_template",
                "string_template": {
                    "template": 'Info',
                    "tag": "h6",
                    "styling": {
                        "classes": ["m-0"]
                    }
                }
            }),
            "table": [
                ["Expectation Suite Name", expectation_suite_name],
                ["Great Expectations Version", ge_version]
            ],
            "styling": {
                "classes": ["col-12", "table-responsive", "mt-1"],
                "body": {
                    "classes": ["table", "table-sm"]
                }
            },
        })

    # TODO: Update tests
    @classmethod
    def _render_expectation_suite_notes(cls, expectations):

        content = []

        total_expectations = len(expectations.expectations)
        columns = []
        for exp in expectations.expectations:
            if "column" in exp.kwargs:
                columns.append(exp.kwargs["column"])
        total_columns = len(set(columns))

        content = content + [
            # TODO: Leaving these two paragraphs as placeholders for later development.
            # "This Expectation suite was first generated by {BasicDatasetProfiler} on {date}, using version {xxx} of Great Expectations.",
            # "{name}, {name}, and {name} have also contributed additions and revisions.",
            "This Expectation suite currently contains %d total Expectations across %d columns." % (
                total_expectations,
                total_columns,
            ),
        ]

        if "notes" in expectations.meta:
            notes = expectations.meta["notes"]
            note_content = None

            if isinstance(notes, string_types):
                note_content = [notes]

            elif isinstance(notes, list):
                note_content = notes

            elif isinstance(notes, dict):
                if "format" in notes:
                    if notes["format"] == "string":
                        if isinstance(notes["content"], string_types):
                            note_content = [notes["content"]]
                        elif isinstance(notes["content"], list):
                            note_content = notes["content"]
                        else:
                            logger.warning("Unrecognized Expectation suite notes format. Skipping rendering.")

                    elif notes["format"] == "markdown":
                        if isinstance(notes["content"], string_types):
                            note_content = [
                                RenderedMarkdownContent(**{
                                    "content_block_type": "markdown",
                                    "markdown": notes["content"],
                                    "styling": {
                                        "parent": {
                                        }
                                    }
                                })
                            ]
                        elif isinstance(notes["content"], list):
                            note_content = [
                                RenderedMarkdownContent(**{
                                    "content_block_type": "markdown",
                                    "markdown": note,
                                    "styling": {
                                        "parent": {
                                        }
                                    }
                                }) for note in notes["content"]
                            ]
                        else:
                            logger.warning("Unrecognized Expectation suite notes format. Skipping rendering.")
                else:
                    logger.warning("Unrecognized Expectation suite notes format. Skipping rendering.")

            if note_content is not None:
                content = content + note_content

        return TextContent(**{
            "content_block_type": "text",
            "header": RenderedStringTemplateContent(**{
                "content_block_type": "string_template",
                "string_template": {
                    "template": 'Notes',
                    "tag": "h6",
                    "styling": {
                        "classes": ["m-0"]
                    }
                }
            }),
            "text": content,
            "styling": {
                "classes": ["col-12", "table-responsive", "mt-1"],
                "body": {
                    "classes": ["table", "table-sm"]
                }
            },
        })


class ProfilingResultsPageRenderer(Renderer):

    def __init__(self, overview_section_renderer=None, column_section_renderer=None):
        if overview_section_renderer is None:
            overview_section_renderer = {
                "class_name": "ProfilingResultsOverviewSectionRenderer"
            }
        if column_section_renderer is None:
            column_section_renderer = {
                "class_name": "ProfilingResultsColumnSectionRenderer"
            }
        module_name = 'great_expectations.render.renderer.other_section_renderer'
        self._overview_section_renderer = instantiate_class_from_config(
            config=overview_section_renderer,
            runtime_environment={},
            config_defaults={
                "module_name": overview_section_renderer.get("module_name", module_name)
            }
        )
        if not self._overview_section_renderer:
            raise ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=overview_section_renderer['class_name']
            )
        module_name = 'great_expectations.render.renderer.column_section_renderer'
        self._column_section_renderer = instantiate_class_from_config(
            config=column_section_renderer,
            runtime_environment={},
            config_defaults={
                "module_name": column_section_renderer.get("module_name", module_name)
            }
        )
        if not self._column_section_renderer:
            raise ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=column_section_renderer['class_name']
            )

    def render(self, validation_results):
        run_id = validation_results.meta['run_id']
        expectation_suite_name = validation_results.meta['expectation_suite_name']
        batch_kwargs = validation_results.meta.get("batch_kwargs")

        # add datasource key to batch_kwargs if missing
        if 'datasource' not in validation_results.meta.get("batch_kwargs", {}):
            # check if expectation_suite_name follows datasource.generator.data_asset_name.suite_name pattern
            if len(expectation_suite_name.split('.')) == 4:
                batch_kwargs['datasource'] = expectation_suite_name.split('.')[0]

        # Group EVRs by column
        # TODO: When we implement a ValidationResultSuite class, this method will move there.
        columns = self._group_evrs_by_column(validation_results)

        ordered_columns = Renderer._get_column_list_from_evrs(validation_results)
        column_types = self._overview_section_renderer._get_column_types(validation_results)

        return RenderedDocumentContent(**{
            "renderer_type": "ProfilingResultsPageRenderer",
            "page_title": run_id + "-" + expectation_suite_name + "-ProfilingResults",
            "expectation_suite_name": expectation_suite_name,
            "utm_medium": "profiling-results-page",
            "batch_kwargs": batch_kwargs,
            "sections":
                [
                    self._overview_section_renderer.render(
                        validation_results,
                        section_name="Overview"
                    )
                ] +
                [
                    self._column_section_renderer.render(
                        columns[column],
                        section_name=column,
                        column_type=column_types.get(column),
                    ) for column in ordered_columns
                ]
        })
