import unittest
from unittest.mock import mock_open, patch
import re
from build_index import load_objects, extract_data, create_gazetteer, get_all_data

# Mocking the global 'all_objs' variable used in 'extract_data'
all_objs_mock = ['Object1', 'Object2', 'Object3']

class TestLoadObjects(unittest.TestCase):

    @patch("builtins.open", new_callable=mock_open, read_data="'Object1'\n'Object2'\n'Object3'")
    def test_valid_file_input(self, mock_file):
        result = load_objects("mock_file.txt")
        self.assertEqual(result, ['Object1', 'Object2', 'Object3'])

    def test_file_not_found_error(self):
        with self.assertRaises(FileNotFoundError):
            load_objects("non_existent_file.txt")

    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_empty_file(self, mock_file):
        result = load_objects("empty_file.txt")
        self.assertEqual(result, [])

class TestExtractData(unittest.TestCase):

    def setUp(self):
        self.data_html = "<title>Test Title</title>\nSample Link\n<span class=\"ext-geocrumbs-breadcrumbs\">...</span>"

    @patch('build_index.all_objs', all_objs_mock)
    def test_valid_html_input(self):
        result = extract_data(self.data_html)
        self.assertIn('title', result)
        self.assertIn('link', result)
        # Additional assertions as needed

    @patch('build_index.all_objs', all_objs_mock)
    def test_malformed_html_input(self):
        result = extract_data("Malformed HTML")
        self.assertEqual(result, {"title": "", "link": "Malformed HTML", "num_cat": 0, "categories": [], "paragraphs": {}, "other": "Malformed HTML"})

    @patch('build_index.all_objs', all_objs_mock)
    def test_no_title_or_link_in_html(self):
        result = extract_data("<span>...</span>")
        self.assertEqual(result['title'], '')
        self.assertEqual(result['link'], '<span>...</span>')

class TestCreateGazetteer(unittest.TestCase):

    def test_valid_dictionary_input(self):
        data = {'key1': 'value1', 'key2': 'value2'}
        with patch("builtins.open", mock_open()) as mock_file:
            create_gazetteer(data)
            mock_file.assert_called_once_with("gazetteer.txt", "w", encoding="utf-8")
            mock_file().write.assert_any_call("key1\n")
            mock_file().write.assert_any_call("key2\n")

    def test_empty_dictionary(self):
        with patch("builtins.open", mock_open()) as mock_file:
            create_gazetteer({})
            mock_file().write.assert_not_called()

class TestGetAllData(unittest.TestCase):

    def test_valid_dictionary_input(self):
        extracted_data = {'title': 'Test Title - Subtitle', 'categories': ['Cat1', 'Cat2 (Subcat)']}
        result = get_all_data(extracted_data)
        self.assertIn('Test Title', result)
        self.assertTrue(result['Cat1'])
        self.assertTrue(result['Cat2'])

    def test_empty_dictionary(self):
        result = get_all_data({})
        self.assertEqual(result, {})

if __name__ == '__main__':
    unittest.main()
