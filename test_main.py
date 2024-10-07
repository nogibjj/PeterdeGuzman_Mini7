"""
Test main.py script
"""

from main import main_results


def test_functions():
    return main_results()


if __name__ == "__main__":
    assert test_functions()["extract_to"] == "/data/pollingplaces_2020.csv"
    assert test_functions()["transform"] == "pollingplaces2020_db"
    assert test_functions()["create"] == "Create Success"
    assert test_functions()["read"] == "Read Success"
    assert test_functions()["update"] == "Update Success"
    assert test_functions()["delete"] == "Delete Success"
