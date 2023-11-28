import unittest
from unittest.mock import mock_open, patch
from build_index import load_objects, extract_data, create_gazetteer, get_all_data

# Mocking the global 'all_objs' variable used in 'extract_data'
all_objs_mock = ['Object1', 'Object2', 'Object3']

class TestLoadObjects(unittest.TestCase):
    """
    Unit test class for testing the `load_objects` function.

    This class contains unit tests to validate the behavior of the `load_objects` function. It tests scenarios including valid file input, file not found error, and empty file input.

    Methods:
    - test_valid_file_input: Tests the function with a mock file containing valid data.
    - test_file_not_found_error: Tests the function's response to a non-existent file.
    - test_empty_file: Tests the function with a mock file that is empty.
    """

    @patch("builtins.open", new_callable=mock_open, read_data="'Object1'\n'Object2'\n'Object3'")
    def test_valid_file_input(self, mock_file):
        """
        Tests `load_objects` function with a mock file containing valid data.

        This test uses a mock file input, patched to contain three objects, to check if the `load_objects` function correctly reads and returns the objects as a list.

        Args:
            mock_file (Mock): The mock file object with preset read data.

        Asserts:
            The function returns a list of objects read from the mock file.
        """
        result = load_objects("mock_file.txt")
        self.assertEqual(result, ['Object1', 'Object2', 'Object3'])

    def test_file_not_found_error(self):
        """
        Tests `load_objects` function with a non-existent file.

        This test checks if the `load_objects` function raises a FileNotFoundError when trying to read from a file that does not exist.

        Asserts:
            FileNotFoundError is raised.
        """
        with self.assertRaises(FileNotFoundError):
            load_objects("non_existent_file.txt")

    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_empty_file(self, mock_file):
        """
        Tests `load_objects` function with an empty mock file.

        This test uses a mock file input, patched to be empty, to check if the `load_objects` function correctly handles an empty file by returning an empty list.

        Args:
            mock_file (Mock): The mock file object with empty read data.

        Asserts:
            The function returns an empty list.
        """
        result = load_objects("empty_file.txt")
        self.assertEqual(result, [])

class TestExtractData(unittest.TestCase):
    """
    Unit test class for testing the `extract_data` function.

    This class contains unit tests to validate the behavior of the `extract_data` function. It tests scenarios including valid HTML input, malformed HTML input, and HTML input with no title or link.

    Methods:
    - setUp: Initializes common test data.
    - test_valid_html_input: Tests the function with valid HTML data.
    - test_malformed_html_input: Tests the function's handling of malformed HTML.
    - test_no_title_or_link_in_html: Tests the function with HTML missing title and link.
    """

    def setUp(self):
        """
        Sets up test data used in the test cases.

        Initializes an HTML string to be used as test data for the `extract_data` function.
        """
        self.data_html = "<title>Test Title</title>\nSample Link\n<span class=\"ext-geocrumbs-breadcrumbs\">...</span>"

    @patch('build_index.all_objs', all_objs_mock)
    def test_valid_html_input(self):
        """
        Tests `extract_data` function with valid HTML data.

        This test checks if the `extract_data` function correctly extracts information from a valid HTML string, including title and link.

        Asserts:
            - The extracted data includes 'title' and 'link'.
            - Additional assertions can be added as needed.
        """
        result = extract_data(self.data_html)
        self.assertIn('title', result)
        self.assertIn('link', result)
        # Additional assertions as needed

    @patch('build_index.all_objs', all_objs_mock)
    def test_malformed_html_input(self):
        """
        Tests `extract_data` function with a malformed HTML string.

        This test verifies the function's ability to handle a malformed HTML string, expecting default values for title, link, categories, etc.

        Asserts:
            The function returns a dictionary with default values for a malformed HTML input.
        """
        result = extract_data("Malformed HTML")
        self.assertEqual(result, {"title": "", "link": "Malformed HTML", "num_cat": 0, "categories": [], "paragraphs": {}, "other": "Malformed HTML"})

    @patch('build_index.all_objs', all_objs_mock)
    def test_no_title_or_link_in_html(self):
        """
        Tests `extract_data` function with HTML that lacks title and link.

        This test checks the function's behavior when provided with HTML content that does not contain a title or link, expecting empty values for these fields.

        Asserts:
            - The 'title' in the result is an empty string.
            - The 'link' in the result is the original HTML string.
        """
        result = extract_data("<span>...</span>")
        self.assertEqual(result['title'], '')
        self.assertEqual(result['link'], '<span>...</span>')

class TestCreateGazetteer(unittest.TestCase):
    """
    Unit test class for testing the `create_gazetteer` function.

    This class contains unit tests to validate the behavior of the `create_gazetteer` function. It tests scenarios including valid dictionary input and empty dictionary input.

    Methods:
    - test_valid_dictionary_input: Tests the function with a non-empty dictionary.
    - test_empty_dictionary: Tests the function with an empty dictionary.
    """

    def test_valid_dictionary_input(self):
        """
        Tests `create_gazetteer` function with a non-empty dictionary.

        This test checks if the `create_gazetteer` function correctly writes the keys of a given dictionary to a file 'gazetteer.txt'. It uses a mock file to intercept file writing operations.

        Asserts:
            - The function calls 'open' to create or open 'gazetteer.txt'.
            - The function writes each key of the dictionary to the file, each on a new line.
        """
        data = {'key1': 'value1', 'key2': 'value2'}
        with patch("builtins.open", mock_open()) as mock_file:
            create_gazetteer(data)
            mock_file.assert_called_once_with("gazetteer.txt", "w", encoding="utf-8")
            mock_file().write.assert_any_call("key1\n")
            mock_file().write.assert_any_call("key2\n")

    def test_empty_dictionary(self):
        """
        Tests `create_gazetteer` function with an empty dictionary.

        This test verifies that the `create_gazetteer` function does not attempt to write to a file when given an empty dictionary. It uses a mock file to check for write operations.

        Asserts:
            - The function does not write anything to the file when the dictionary is empty.
        """
        with patch("builtins.open", mock_open()) as mock_file:
            create_gazetteer({})
            mock_file().write.assert_not_called()

class TestGetAllData(unittest.TestCase):
    """
    Unit test class for testing the `get_all_data` function.

    This class contains unit tests to validate the behavior of the `get_all_data` function. It tests scenarios including valid dictionary input and empty dictionary input.

    Methods:
    - test_valid_dictionary_input: Tests the function with a dictionary containing title and categories.
    - test_empty_dictionary: Tests the function with an empty dictionary.
    """

    def test_valid_dictionary_input(self):
        """
        Tests `get_all_data` function with a dictionary containing title and categories.

        This test verifies the function's ability to process and clean a given dictionary's 'title' and 'categories' entries. It checks if the function correctly removes specific characters and newline characters from the title and categories, and then returns them as keys in a new dictionary with values set to True.

        Asserts:
            - The returned dictionary contains cleaned 'title' and 'categories' as keys.
            - The values corresponding to these keys are True.
        """
        extracted_data = {'title': 'Test Title - Subtitle', 'categories': ['Cat1', 'Cat2 (Subcat)']}
        result = get_all_data(extracted_data)
        self.assertIn('Test Title', result)
        self.assertTrue(result['Cat1'])
        self.assertTrue(result['Cat2'])

    def test_empty_dictionary(self):
        """
        Tests `get_all_data` function with an empty dictionary.

        This test checks if the `get_all_data` function returns an empty dictionary when provided with an empty input dictionary, indicating that there is no data to process.

        Asserts:
            The function returns an empty dictionary when the input is empty.
        """
        result = get_all_data({})
        self.assertEqual(result, {})

if __name__ == '__main__':
    unittest.main()
