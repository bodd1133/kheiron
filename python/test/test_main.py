import pytest
from pyspark import Row
from main import add_pass_col, find_most_popular_subjects, find_unique_subjects, find_pass_rate_per_university

@pytest.mark.usefixtures("spark")
def test_add_pass_column_numeric(spark):
    input_df = spark.createDataFrame(
        [
            Row(
                university="3",
                subject="A",
                grade="90",
            ),
            Row(
                university="4",
                subject="A",
                grade="40",
            ),
            Row(
                university="2",
                subject="D",
                grade="",
            ),
        ]
    )

    expected = spark.createDataFrame(
        [
            Row(
                university="3",
                subject="A",
                grade="90",
                course_passed=True,
            ),
            Row(
                university="4",
                subject="A",
                grade="40",
                course_passed=False,
            ),
            Row(
                university="2",
                subject="D",
                grade="40",
                course_passed=False,
            ),
        ]
    )

    actual = add_pass_col(input_df)
    assert actual.collect() == expected.collect()

# @pytest.mark.usefixtures("spark")
# def test_add_pass_column_string(spark):
#     input_df = spark.createDataFrame(
#         [
#             Row(
#                 university="1",
#                 subject="A",
#                 grade="A+",
#             ),
#             Row(
#                 university="2",
#                 subject="A",
#                 grade="E",
#             ),
#         ]
#     )

#     expected = spark.createDataFrame(
#         [
#             Row(
#                 university="1",
#                 subject="A",
#                 grade="A+",
#                 course_passed=True,
#             ),
#             Row(
#                 university="2",
#                 subject="A",
#                 grade="E",
#                 course_passed=False,
#             ),
#         ]
#     )

#     actual = add_pass_col(input_df)
#     assert actual.collect() == expected.collect()

# @pytest.mark.usefixtures("spark")
# def test_find_most_popular_subject(spark):
#     input_df = spark.createDataFrame(
#         [
#             Row(
#                 university="2",
#                 subject="C",
#                 course_passed=True,
#             ),
#             Row(
#                 university="3",
#                 subject="D",
#                 course_passed=True,
#             ),
#             Row(
#                 university="1",
#                 subject="D",
#                 course_passed=True,
#             ),
#             Row(
#                 university="1",
#                 subject="A",
#                 course_passed=True,
#             ),
#             Row(
#                 university="1",
#                 subject="A",
#                 course_passed=False,
#             ),
#             Row(
#                 university="2",
#                 subject="A",
#                 course_passed=True,
#             ),
#             Row(
#                 university="2",
#                 subject="B",
#                 course_passed=True,
#             ),
#             Row(
#                 university="2",
#                 subject="B",
#                 course_passed=True,
#             ),
#         ]
#     )

#     expected = spark.createDataFrame(
#         [
#             Row(
#                 subject="A",
#                 count=3,
#             ),
#             Row(
#                 subject="B",
#                 count=2,
#             ),
#             Row(
#                 subject="D",
#                 count=2,
#             ),
#         ]
#     )

#     actual = find_most_popular_subjects(input_df)
#     print(actual)
#     print(expected.collect())
#     assert actual == expected.collect()

# @pytest.mark.usefixtures("spark")
# def test_find_unique_subject(spark):
#     input_df = spark.createDataFrame(
#         [
#             Row(
#                 university="2",
#                 subject="C",
#                 course_passed=True,
#             ),
#             Row(
#                 university="3",
#                 subject="C",
#                 course_passed=True,
#             ),
#             Row(
#                 university="1",
#                 subject="D",
#                 course_passed=True,
#             ),
#             Row(
#                 university="1",
#                 subject="A",
#                 course_passed=True,
#             ),
#             Row(
#                 university="2",
#                 subject="F",
#                 course_passed=False,
#             ),
#             Row(
#                 university="2",
#                 subject="A",
#                 course_passed=True,
#             ),
#             Row(
#                 university="3",
#                 subject="U",
#                 course_passed=True,
#             ),
#             Row(
#                 university="3",
#                 subject="U",
#                 course_passed=True,
#             ),
#         ]
#     )

#     expected = spark.createDataFrame(
#         [
#             Row(
#                 university="1",
#                 subject="D",
#                 count=1,
#             ),
#             Row(
#                 university="2",
#                 subject="F",
#                 count=1,
#             ),
#             Row(
#                 university="3",
#                 subject="U",
#                 count=2,
#             ),
#         ]
#     )

#     actual = find_unique_subjects(input_df)
#     actual.collect() == expected.collect()

# @pytest.mark.usefixtures("spark")
# def test_find_pass_rate(spark):
#     input_df = spark.createDataFrame(
#         [
#             Row(
#                 university="2",
#                 subject="C",
#                 course_passed=True,
#                 total_enrolments=4,
#             ),
#             Row(
#                 university="2",
#                 subject="C",
#                 course_passed=False,
#                 total_enrolments=4,
#             ),
#             Row(
#                 university="1",
#                 subject="D",
#                 course_passed=True,
#                 total_enrolments=2,
#             ),
#             Row(
#                 university="1",
#                 subject="A",
#                 course_passed=True,
#                 total_enrolments=2,
#             ),
#             Row(
#                 university="2",
#                 subject="F",
#                 course_passed=False,
#                 total_enrolments=4,
#             ),
#             Row(
#                 university="2",
#                 subject="A",
#                 course_passed=True,
#                 total_enrolments=4,
#             ),
#             Row(
#                 university="3",
#                 subject="U",
#                 course_passed=True,
#                 total_enrolments=2,
#             ),
#             Row(
#                 university="3",
#                 subject="U",
#                 course_passed=False,
#                 total_enrolments=2,
#             ),
#         ]
#     )
#     expected = spark.createDataFrame(
#         [
#             Row(
#                 university="2",
#                 total_enrolments=4,
#                 pass_count=2,
#                 prop_of_total=0.5
#             ),
#             Row(
#                 university="1",
#                 total_enrolments=2,
#                 pass_count=2,
#                 prop_of_total=1.0,
#             ),
#             Row(
#                 university="3",
#                 total_enrolments=2,
#                 pass_count=1,
#                 prop_of_total=0.5
#             ),
#         ]
#     )

#     actual = find_pass_rate_per_university(input_df)
#     assert actual.collect() == expected.collect()