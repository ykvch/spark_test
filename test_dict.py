import pytest


# func to test
def transform_data(records):
    for record in records:
        record["amount"] = record["amount"] / 100
    return records


@pytest.mark.parametrize(
    "test_data, exc_type, exc_arg",
    [
        ([{}], KeyError, "amount"),
        ([{"amount": ...}], TypeError, "unsupported operand type"),
    ],
)
def test_negative(test_data, exc_type, exc_arg):
    with pytest.raises(exc_type) as exc_info:
        transform_data(test_data)

    assert exc_arg in exc_info.value.args[0]


def test_empty():  # not a dict here
    assert transform_data([]) == []

@pytest.mark.parametrize("test_data, exp_result", [
    ([{"amount": 1}], [0.01]),  # pytest handles floats eq unless some extremities
    ([{"amount": 0}], [0]),
    ([{"amount": -1}, {"amount": 100}], [-.01, 1])
])
def test_positive(test_data, exp_result):
    ret_vals = [i["amount"] for i in transform_data(test_data)]

    assert ret_vals == exp_result