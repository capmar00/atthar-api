import requests
import re
import os
import glob
from xml.etree import ElementTree as ET
import json
from collections import defaultdict

from modules.shared import *

useful_dataflow_ids = ['22_289']

#################################################
####### json and jsonl functions  ###############
#################################################
def save_as_json(data, file_name):
    """
    Saves a dictionary to a file in JSON format.

    Args:
        data (dict): The dictionary to be saved.
        file_path (str): The path to the file where the data should be saved.

    Raises:
        IOError: If there is an issue opening or writing to the file, an error message will be printed.
    """
    file_path = DATA_ISTAT_API_PATH + "/" + file_name
    try:
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        print(f"Data successfully saved to {file_path}")
    except IOError as e:
        print(f"An error occurred while writing the file: {e}")


def save_as_jsonl(data, file_name):
    """
    Saves a list of dictionaries to a file in JSON Lines (JSONL) format.

    Each dictionary in the provided list `data` is serialized to a JSON object
    and written to the specified file, with one JSON object per line.

    Args:
        data (list): A list of dictionaries to be saved as JSON Lines.
        file_path (str): The path to the file where the data should be saved.

    Raises:
        IOError: If there is an issue opening or writing to the file, an error
                 message will be printed.

    Example:
        save_as_jsonl([{'key1': 'value1'}, {'key2': 'value2'}], 'output.jsonl')
        This will create a file 'output.jsonl' with the following content:
        {"key1": "value1"}
        {"key2": "value2"}
    """
    file_path = DATA_ISTAT_API_PATH + "/" + file_name
    try:
        with open(file_path, 'w', encoding='utf-8') as file:
            for entry in data:
                json_record = json.dumps(entry, ensure_ascii=False)
                file.write(json_record + '\n')
        print(f"Data successfully saved to {file_path}")
    except IOError as e:
        print(f"An error occurred while writing the file: {e}")


def read_jsonl_file(file_name, type='str'):
    """
    Fetches a list of ISTAT datasets from a JSONL file and returns the data as a formatted JSON string.

    This function reads a JSON Lines (JSONL) file. It processes each line in the file,
    which represents a JSON object, and compiles these objects into a list. The list is then
    converted into a JSON string with pretty formatting (indented by 2 spaces) and returned.

    Returns:
        str: A JSON string representing the list of datasets from the JSONL file.
    """
    data_list = []
    file_path = DATA_ISTAT_API_PATH + "/" + file_name
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            data = json.loads(line.strip())
            data_list.append(data)
    if type == 'str':
        data_str = json.dumps(data_list, indent=2)  # JSON string
        clean_data_str = remove_newlines_and_spaces(data_str)
        return clean_data_str
    elif type == 'list':
        return data_list


def assemble_locations():
    file_names = ['_geographic_areas', '_regions', '_provinces']
    locations = []  # Initialize the list to store results
    for name in file_names:
        file_path = DATA_ISTAT_API_PATH + "/ITTER107/" + name + ".jsonl"
        with open(file_path, 'r', encoding='utf-8') as file:  # Ensure proper handling of the file opening
            for line in file:
                data = json.loads(line)
                locations.append(data)
    return locations



#######################################################
############## transformation functions ###############
#######################################################

def clean_dataflow_id(input_string: str) -> str:
    """ Use a regular expression to keep only digits and underscores """
    cleaned_string: str = re.sub(r'[^\d_]', '', input_string)
    if cleaned_string:
        return cleaned_string
    return None


def clean_location_type(input_string):
    """
    Cleans the input string by returning the first occurrence of any of the allowed words:
    '_geographic_areas', '_regions', '_provinces', '_municipalities'.

    Args:
        input_string (str): The input string to be cleaned.

    Returns:
        str: The cleaned string containing only the matched word, or None if no match is found.
    """
    allowed_words = ['_geographic_areas', '_regions', '_provinces', '_municipalities']
    for word in allowed_words:
        if word in input_string:
            return word
    return None


def remove_newlines_and_spaces(s):
    # Replace newlines with empty strings
    s = s.replace('\n', '')
    # Remove all spaces
    s = s.replace(' ', '')
    return s

def get_location_type_files(location_type, first_letter=None):
    """
    Retrieves data from a JSONL file based on the specified location type.

    This function takes a location type as input and returns the contents of the corresponding JSONL file.
    The location type can be one of the following: '_geographic_areas', '_regions', '_provinces', or '_municipalities'.
    The function reads the appropriate file from the "ITTER107" directory and returns its contents.

    Args:
        location_type (str): The type of location for which the data is to be retrieved.
                             Valid values are '_geographic_areas', '_regions', '_provinces', and '_municipalities'.

    Returns:
        list: A list of dictionaries representing the contents of the JSONL file. If the location type is not recognized,
    """
    location_type_file = None
    if location_type == '_geographic_areas':
        location_type_file = read_jsonl_file("ITTER107/_geographic_areas.jsonl")
    elif location_type == '_regions':
        location_type_file = read_jsonl_file("ITTER107/_regions.jsonl")
    elif location_type == '_provinces':
        location_type_file = read_jsonl_file("ITTER107/_provinces.jsonl")
    elif location_type == '_municipalities':
        location_type_file = read_jsonl_file(f"ITTER107/_municipalities{first_letter}.jsonl")
    return location_type_file


def extract_dataflow_id_and_name(data_list):
    # If the input is a string, convert it to a list of dictionaries
    if isinstance(data_list, str):
        data_list = json.loads(data_list)
    return [{"dataflow_id": data["dataflow_id"], "name": data["name"]} for data in data_list]


def clean_location_id(input_string):
    """
    Cleans the input string by extracting the part that matches one of the specified patterns.

    The patterns are:
        1. r'\b(ITC|ITD|ITE|ITF|ITG)\b|^IT\b'
        2. r'\b(ITC|ITD|ITE|ITF|ITG)\d\b'
        3. r'\b(ITC|ITD|ITE|ITF|ITG)\d{2}\b|IT111'
        4. r'\b\d{6}\b'

    Args:
        input_string (str): The input string to be cleaned.

    Returns:
        str: The cleaned string if a match is found, otherwise None.
    """
    patterns = [
        r'\b(ITC|ITD|ITE|ITF|ITG)\b|^IT\b',
        r'\b(ITC|ITD|ITE|ITF|ITG)\d\b',
        r'\b(ITC|ITD|ITE|ITF|ITG)\d{2}\b|IT111',
        r'\b\d{6}\b'
    ]
    for pattern in patterns:
        match = re.search(pattern, input_string)
        if match:
            return match.group(0)
    return None


def get_version_by_dataflow_id(dataflow_id):
    data = read_jsonl_file("useful_istat_datasets.jsonl", "list")
    for entry in data:
        if entry["dataflow_id"] == dataflow_id:
            return entry["version"]
    return None


def transform_age_code(age_code):
    if age_code == 'TOTAL':
        return 'total'
    elif age_code == 'Y_GE100':
        return '100+'
    # Handle the regular case for age codes like 'Y0', 'Y1', ..., 'Y99'
    elif age_code.startswith('Y') and age_code[1:].isdigit():
        return str(int(age_code[1:]))  # Convert 'Y1' to '1', 'Y99' to '99'
    return None

########################################################################################
################# API functions  #######################################################
######## encapsulated functions with underscore to indicate private/internal use #######
########################################################################################
def query_api(url):
    """
        Sends a GET request to the specified URL and returns the response content as a string.

        The function attempts to fetch the content from the provided URL using a GET request.
        It handles HTTP errors and returns the content of the response if successful.
        The response encoding is explicitly set to 'utf-8'.

        Args:
            url (str): The URL to which the GET request is sent.

        Returns:
            str: The content of the response as a string if the request is successful.
            None: If an error occurs during the request, the function returns None and prints an error message.

        Raises:
            requests.RequestException: If there is an issue with the request, such as a network problem or
                                       a non-2xx HTTP status code, an error message is printed.
        """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        response.encoding = 'utf-8'
        return response.text
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return None


def _parse_dataflows(xml_data):
    try:
        root = ET.fromstring(xml_data)
        ns = {
            'mes': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
            'str': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
            'com': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
            'xml': 'http://www.w3.org/XML/1998/namespace'
        }
        dataflows = root.find('.//mes:Structures/str:Dataflows', ns)
        results = []
        if dataflows is not None:
            for dataflow in dataflows.findall('str:Dataflow', ns):
                dataflow_id = dataflow.get('id')
                version = dataflow.get('version')
                name_element = dataflow.find('.//com:Name[@xml:lang="en"]', ns)
                name = name_element.text if name_element is not None else "No English name"
                if name == "No English name":
                    name_element = dataflow.find('.//com:Name[@xml:lang="it"]', ns)
                    name = name_element.text if name_element is not None else "No English and Italian name"
                ref_element = dataflow.find('.//str:Structure/Ref', ns)
                datastructure_id = ref_element.get('id') if ref_element is not None else "No Ref ID"
                results.append({
                    'dataflow_id': dataflow_id,
                    'version': version,
                    'name': name,
                    'datastructure_id': datastructure_id
                })
        return results
    except ET.ParseError as e:
        print(f"XML parsing error: {e}")
        return None


def _filter_and_save_dataflows(dataflows, filter_ids, file_name):
    filtered_dataflows = [df for df in dataflows if df['dataflow_id'] in filter_ids]
    save_as_jsonl(filtered_dataflows, file_name)


def _fetch_codelist_name(ref_id):
    url = f"https://esploradati.istat.it/SDMXWS/rest/codelist/IT1/{ref_id}"
    xml_data = query_api(url)
    if xml_data:
        root = ET.fromstring(xml_data)
        name_element = root.find('.//com:Name[@xml:lang="en"]', {
            'com': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
            'xml': 'http://www.w3.org/XML/1998/namespace'
        })
        return name_element.text if name_element is not None else "Name not found"



def fetch_parse_and_save_dataflows():
    """
        Parses XML data to extract information about dataflows.

        The function processes the provided XML string to extract details about dataflows,
        including their ID, version, English name, and associated data structure ID. It returns
        a list of dictionaries containing this information.

        Args:
            xml_data (str): A string containing the XML data to be parsed.

        Returns:
            list: A list of dictionaries, where each dictionary represents a dataflow and
                  contains the following keys:
                  - 'dataflow_id': The ID of the dataflow.
                  - 'version': The version of the dataflow.
                  - 'name': The English name of the dataflow (or "No English name" if not found).
                  - 'datastructure_id': The associated data structure ID (or "No Ref ID" if not found).
            None: If an XML parsing error occurs, the function returns None and prints an error message.

        Raises:
            xml.etree.ElementTree.ParseError: If the XML data cannot be parsed, an error message is printed.
        """
    url = "https://esploradati.istat.it/SDMXWS/rest/dataflow/IT1/"
    xml_data = query_api(url)
    if xml_data:
        dataflows = _parse_dataflows(xml_data)
        if dataflows:
            save_as_jsonl(dataflows, "all_istat_datasets.jsonl")
            _filter_and_save_dataflows(dataflows, useful_dataflow_ids, "useful_istat_datasets.jsonl")


def fetch_parse_and_save_datastructure(structure_ref):
    """
    Fetches, parses, and returns details of a data structure from an API.

    The function sends a GET request to the API using the provided data structure reference ID (`structure_ref`).
    It retrieves the XML response, parses it to extract information about dimensions and their corresponding
    enumeration references, and returns these details as a list of dictionaries.

    Args:
        structure_ref (str): The reference ID of the data structure to be fetched and parsed.

    Returns:
        list: A list of dictionaries, where each dictionary represents a dimension and contains the following keys:
              - 'dimension': The ID of the dimension.
              - 'dimension_id': The ID of the enumeration reference associated with the dimension.
              - 'description': The English name/description of the enumeration reference.
        None: If the XML data cannot be retrieved or parsed, the function returns None.

    Raises:
        requests.RequestException: If there is an issue with the API request, handled by the `query_api` function.
        xml.etree.ElementTree.ParseError: If the XML data cannot be parsed, handled by the XML parsing functions.
    """
    url = f"https://esploradati.istat.it/SDMXWS/rest/datastructure/IT1/{structure_ref}"
    xml_data = query_api(url)
    if xml_data:
        root = ET.fromstring(xml_data)
        ns = {
            'mes': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
            'str': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
            'com': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'
        }
        results = []
        dimensions = root.findall('.//str:DataStructureComponents/str:DimensionList/str:Dimension', ns)
        for dimension in dimensions:
            dim_id = dimension.get('id')
            enumeration_refs = dimension.findall('.//str:Enumeration/Ref', ns)
            for ref in enumeration_refs:
                ref_id = ref.get('id')
                name_en = _fetch_codelist_name(ref_id)
                if name_en:
                    results.append({
                        'dimension': dim_id,
                        'dimension_id': ref_id,
                        'description': name_en
                    })
        return results


def get_dataset_info_by_dataflow_id(dataflow_id_value):
    """
    Retrieves dataset information from a JSON Lines (JSONL) file based on the specified dataflow ID.

    The function reads through a JSONL file containing dataset information, searching for an entry that
    matches the given `dataflow_id_value`. When a match is found, it returns the corresponding dataset
    information as a dictionary.

    Args:
        dataflow_id_value (str): The ID of the dataflow for which information is to be retrieved.

    Returns:
        dict: A dictionary containing the dataset information for the specified dataflow ID.
              If no matching dataflow ID is found, an empty dictionary is returned.

    Raises:
        IOError: If there is an issue reading the JSONL file, an error will be raised by the `open` function.
    """
    result = {}
    file_path = DATA_ISTAT_API_PATH + '/useful_istat_datasets.jsonl'
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            row = json.loads(line)
            if row['dataflow_id'] == dataflow_id_value:
                result = row
                break
    return result



def get_datastructures(dataflow_id):
    """
    Retrieves dataset information, processes the associated data structure, saves the results to a JSONL file,
    and returns the results as a JSON string.

    The function first retrieves dataset information corresponding to the specified `dataflow_id`. It then fetches
    and parses the data structure related to the dataset. Finally, it saves the parsed data structure details
    to a JSON Lines (JSONL) file.

    Args:
        dataflow_id (str): The ID of the dataflow for which the data structure is to be processed and saved.

    Returns:
        str: A JSON string of the results or None if the data structures are not available.

    Raises:
        IOError: If there is an issue reading the JSONL file or writing the output file.
        requests.RequestException: If there is an issue with the API request when fetching the data structure.
        xml.etree.ElementTree.ParseError: If the XML data cannot be parsed when processing the data structure.
    """
    try:
        selected_dataset_info = get_dataset_info_by_dataflow_id(dataflow_id)
        if selected_dataset_info:
            datastructures = fetch_parse_and_save_datastructure(selected_dataset_info['datastructure_id'])
            # Filter new datastructures to avoid duplicates based on dimension_id
            for datastructure in datastructures:
                if datastructure['dimension_id'] not in dimensions_seen:
                    useful_datastructures.append(datastructure)
                    dimensions_seen.add(datastructure['dimension_id'])
            if datastructures:
                # _get_datastructure_xml_files(dataflow_id, datastructures)
                save_as_jsonl(datastructures, f"{selected_dataset_info['dataflow_id']}__datastructures.jsonl")
                return json.dumps(datastructures, indent=2)  # JSON string
    except (IOError, requests.RequestException, ET.ParseError) as e:
        print(f"Error occurred: {e}")
    return None


def save_codelist_as_xml(dimension_id):
    """
    Fetches XML content from the specified URL and saves it to a file.

    The function sends a GET request to the provided URL to retrieve XML content.
    It then saves this content to a file with the specified file_name.

    Args:
        url (str): The URL from which to fetch the XML content.
        dimension_id (str): The path and name of the file where the XML content will be saved.

    Returns:
        None

    Raises:
        IOError: If there is an issue writing the XML content to the file.
        requests.RequestException: If there is an issue with the API request when fetching the XML content.
    """
    url = f"https://esploradati.istat.it/SDMXWS/rest/codelist/IT1/{dimension_id}"
    xml_content = query_api(url)
    file_path = f"{DATA_ISTAT_API_PATH}/xml/{dimension_id}.xml"
    if xml_content:
        try:
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(xml_content)
            print(f"XML content successfully saved to {dimension_id}.xml")
        except IOError as e:
            print(f"An error occurred while writing the XML file: {e}")


def get_constraints_list_from_dimension(file_path, target_dimension_id):
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            data = json.loads(line.strip())
            if data.get("dimension_id") == target_dimension_id:
                return data.get("constraints")
    return None


def save_CL_ITTER107_codelist_as_jsonl(extraction_type, dataflow_id):
    """
    Queries an API to fetch XML data for a specific data flow ID and processes this data based on the
    extraction type to extract and save specific geographic codes as JSONL files.

    The function uses a regex pattern based on the `extraction_type` to filter and extract the 'id' and Italian names
    of the geographic codes. Each code is then saved to a JSON Lines (JSONL) file named according to the extraction type.

    Args:
        extraction_type (str): The type of extraction to perform, which affects the regex pattern used for filtering:
                               "A" for geographic areas, "M" for municipalities, "R" for regions, and "P" for provinces.
        dataflow_id (str): Identifier for the data flow from which to fetch and process XML data.

    Returns:
        None: Prints a success message upon completion or an error message if exceptions occur.

    Raises:
        IOError: If there is an issue writing the JSONL file.
        xml.etree.ElementTree.ParseError: If there are issues parsing the XML data.
    """
    url = "https://esploradati.istat.it/SDMXWS/rest/codelist/IT1/CL_ITTER107"
    xml_data = query_api(url)
    CL_ITTER107_constraints = get_constraints_list_from_dimension(f"{DATA_ISTAT_API_PATH}/{dataflow_id}__constraints.jsonl", "CL_ITTER107")
    if xml_data:
        try:
            root = ET.fromstring(xml_data)
            ns = {
                'structure': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
                'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
                'xml': 'http://www.w3.org/XML/1998/namespace'
            }
            results = []
            municipalities_by_letter = {}
            # Define the pattern and file name based on extraction type
            if extraction_type == "A":
                pattern = r'^(ITC|ITD|ITE|ITF|ITG)$|^IT$'  # IT Italy
                file_name = "_geographic_areas.jsonl"
            elif extraction_type == "R":
                # include ITDA Trentino Alto Adige
                # exclude ITD1 Provincia Autonoma di Bolzano, ITD2 Provincia Autonoma di Trento
                pattern = r'^(ITC|ITE|ITF|ITG)\d$|^(ITD)(?!1$|2$)\d$|^ITDA$'
                file_name = "_regions.jsonl"
            elif extraction_type == "P":
                # include IT111 Sud Sardegna, IT108 Monza e Brianza, IT110 Barletta-Andria-Trani, IT109 Fermo, ITC4A Cremona, ITE1A Grosseto, ITC4B Mantova
                # exclude ITG29 Olbia-Tempio
                pattern = r"^(ITC|ITD|ITE|ITF)(\d{2})$|^(ITG)(?!29$)\d{2}$|^IT111$|^IT108$|^IT110$|^IT109$|^ITC4A$|^ITE1A$|^ITC4B$"
                file_name = "_provinces.jsonl"
            elif extraction_type == "M":
                pattern = r'^\d{6}$'
                file_name = "_municipalities.jsonl"
            else:
                print("Invalid extraction type provided.")
                return
            # Find all 'structure:Code' elements in the XML
            for code in root.findall('.//structure:Code', ns):
                code_id = code.get('id')
                if code_id in CL_ITTER107_constraints:
                    # Check if the 'structure:Code id' matches the pattern
                    if re.match(pattern, code_id):
                        italian_name_element = code.find('common:Name[@xml:lang="it"]', ns)
                        italian_name = italian_name_element.text if italian_name_element is not None else None
                        if code_id == "ITC2" and italian_name == "Valle d'Aosta / Vallée d'Aoste":
                            italian_name = "Valle d'Aosta"
                        if code_id == "ITC20" and italian_name == "Valle d'Aosta / Vallée d'Aoste":
                            italian_name = "Aosta"
                        if code_id == "ITD10" and italian_name == "Bolzano / Bozen":
                            italian_name = "Bolzano"
                        if code_id == "ITDA" and italian_name == "Trentino Alto Adige / Südtirol":
                            italian_name = "Trentino Alto Adige"
                        result = {
                            italian_name: code_id
                        }
                        results.append(result)
                        # If municipalities, group by the first letter
                        if extraction_type == "M" and italian_name:
                            first_letter = italian_name[0].upper()
                            if first_letter not in municipalities_by_letter:
                                municipalities_by_letter[first_letter] = []
                            municipalities_by_letter[first_letter].append(result)
            # Sort the results by 'italian_name' key before saving
            results_sorted = sorted(results, key=lambda x: list(x.keys())[0], reverse=False)
            # Save the general JSONL file
            file_path = f"{DATA_ISTAT_API_PATH}/ITTER107/{file_name}"
            with open(file_path, 'w', encoding='utf-8') as file:
                for result in results_sorted:
                    json_record = json.dumps(result, ensure_ascii=False)
                    file.write(json_record + '\n')
            print(f"Data successfully saved to {file_name}")
            # Save individual files for each letter if municipalities were selected
            if extraction_type == "M":
                for letter, municipalities in municipalities_by_letter.items():
                    letter_file_path = f"{DATA_ISTAT_API_PATH}/ITTER107/_municipalities_{letter}.jsonl"
                    with open(letter_file_path, 'w', encoding='utf-8') as letter_file:
                        for municipality in sorted(municipalities, key=lambda x: list(x.keys())[0], reverse=False):
                            json_record = json.dumps(municipality, ensure_ascii=False)
                            letter_file.write(json_record + '\n')
                    print(f"Data successfully saved to _municipalities_{letter}.jsonl")
        except ET.ParseError as e:
            print(f"XML parsing error: {e}")
        except IOError as e:
            print(f"An error occurred while writing the JSONL file: {e}")


def save_codelist_as_jsonl(dataflow_id, dimension_id):
    """
    Queries an API to fetch XML data for a specific data flow ID and processes this data to extract
    the 'id' and English names of codes constrained by the data flow, saving the results in a JSONL file.

    This function assumes the XML has specific nodes ('structure:Code') and attempts to extract the 'id' attribute and the
    associated English name ('common:Name xml:lang="en"'). The results are then sorted by the English name and saved in a JSONL file
    formatted with one JSON object per line.

    Args:
        dataflow_id (str): Identifier for the data flow from which to fetch and process XML data.

    Returns:
        None: This function prints a success message upon completion or error messages if exceptions occur.

    Raises:
        IOError: If there is an issue writing to the JSONL file.
        xml.etree.ElementTree.ParseError: If there are issues parsing the XML data.
    """
    url = f"https://esploradati.istat.it/SDMXWS/rest/codelist/IT1/{dimension_id}"
    xml_data = query_api(url)
    dim_constraints = get_constraints_list_from_dimension(f"{DATA_ISTAT_API_PATH}/{dataflow_id}__constraints.jsonl", dimension_id)
    if xml_data:
        try:
            root = ET.fromstring(xml_data)
            ns = {
                'structure': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
                'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
                'xml': 'http://www.w3.org/XML/1998/namespace'
            }
            results = []
            file_name = f"{dataflow_id}_{dimension_id}__codelist.jsonl"
            # Find all 'structure:Code' elements in the XML
            for code in root.findall('.//structure:Code', ns):
                code_id = code.get('id')
                if code_id in dim_constraints:
                    element = code.find('common:Name[@xml:lang="en"]', ns)
                    el_name = element.text if element is not None else None
                    result = {
                        el_name: code_id
                    }
                    results.append(result)
            results_sorted = sorted(results, key=lambda x: list(x.keys())[0], reverse=False)
            # Save the general JSONL file
            file_path = f"{DATA_ISTAT_API_PATH}/{file_name}"
            with open(file_path, 'w', encoding='utf-8') as file:
                for result in results_sorted:
                    json_record = json.dumps(result, ensure_ascii=False)
                    file.write(json_record + '\n')
            print(f"Data successfully saved to {file_name}")
        except ET.ParseError as e:
            print(f"XML parsing error: {e}")
        except IOError as e:
            print(f"An error occurred while writing the JSONL file: {e}")


def _fetch_dimension_xml(dimension_id):
    """
    Fetches XML data for a given dimension ID using the query_api function.

    Args:
        dimension_id (str): The ID of the dimension to query.

    Returns:
        str: The XML data as a string, or None if an error occurred.
    """
    if dimension_id != "ITTER107":
        url = f"https://esploradati.istat.it/SDMXWS/rest/codelist/IT1/{dimension_id}"
        xml_data = query_api(url)
        return xml_data
    return None


def _get_datastructure_xml_files(dataflow_id: str, datastructures: list):
    """
    Reads a JSONL file, gets the dimension IDs,
    fetches corresponding XML data, and saves them as a single XML file.

    Returns:
        None
    """
    output_file_name = f"{DATA_ISTAT_API_PATH}/xml/{dataflow_id}_constraints.xml"
    # Initialize an empty list to collect all XML contents
    all_xml_data = []
    for item in datastructures:
        dimension_id = item.get("dimension_id")
        if dimension_id:
            xml_data = _fetch_dimension_xml(dimension_id)
            if xml_data:
                all_xml_data.append(xml_data)
    # Write all collected XML data to a single file
    if all_xml_data:
        try:
            with open(output_file_name, 'w', encoding='utf-8') as output_file:
                for xml_content in all_xml_data:
                    output_file.write(xml_content)
            print(f"XML data successfully saved to {output_file_name}")
        except IOError as e:
            print(f"An error occurred while writing the XML file: {e}")


def get_constraints(dataflow_id):
    """
    Fetches and saves constraints along with dimension metadata for a given dataflow ID.

    Args:
        dataflow_id (str): The ID of the dataflow to query constraints for.

    Returns:
        None: Function saves the combined information into a JSON Lines (JSONL) file.
    """
    # Fetch dimension metadata
    selected_dataset_info = get_dataset_info_by_dataflow_id(dataflow_id)
    datastructures = fetch_parse_and_save_datastructure(selected_dataset_info['datastructure_id'])
    # Define namespaces
    ns = {
        'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
        'structure': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
        'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'
    }
    # Fetch constraints and parse them
    # skip_id = "CL_ITTER107"
    for dim in datastructures:
        # if dim['dimension_id'] == skip_id:
            # continue
        constraints_url = f"https://esploradati.istat.it/SDMXWS/rest/codelist/IT1/{dim['dimension_id']}"
        constraints_xml_data = query_api(constraints_url)
        if constraints_xml_data:
            # Parse the XML string into an ElementTree object
            try:
                root = ET.fromstring(constraints_xml_data)
                # Extract and store constraints
                codes = root.findall('.//structure:Code', ns)
                constraints = [code.get('id') for code in codes if code.get('id') is not None]
                if constraints:
                    dim['constraints'] = constraints
            except ET.ParseError as e:
                print(f"Error parsing XML for dimension {dim['dimension_id']}: {e}")
    # Save the datastructures with constraints as JSONL
    save_as_jsonl(datastructures, f"{dataflow_id}__constraints.jsonl")


######################################################
############## LLM TOOLS and FUNCTIONS ###############
######################################################

tools=[
    {
      "type": "function",
      "function": {
        "name": "fetch_population_for_locations_years_sex_age_via_sdmx",
        "description": "Fetches population data for specific locations, sex categories, age and time periods using the Istat SDMX web service. Supports multiple locations and sex categories.",
        "parameters": {
          "type": "object",
          "properties": {
            "location_ids": {
              "type": "string",
              "description": "Geographical identifiers for the locations, concatenated by '+' if multiple, e.g., 'ITC+ITE2+ITF14'"
            },
            "sex": {
              "type": "string",
              "description": "The sex category for which data is requested. '1' for male, '2' for female, '9' for total. Can be combined with '+', e.g., '1+2+9'"
            },
            "age": {
                  "type": "string",
                      "description": "The age in years for which data is requested. Follow these rules to interpret the age definition: 1. Exact age (e.g., `X years`): Format: `YX`; Example: `0 year` → `Y0`, `1 year` → `Y1`, `10 years` → `Y10`. 2. Age and over (e.g., `X years and over`): Format: `Y_GEX`; Example: `14 years and over` → `Y_GE14`. 3. Until a certain age (e.g., `until X years` or `under X years`): Format: `Y_UNX`; Example: `until 15 years` → `Y_UN15`. 4. Age ranges (e.g., `X-Y years`): Format: `YX-Y`; Example: `14-15 years` → `Y14-15`. 5. TOTAL (representing all ages): Use the code `TOTAL`."
            },
            "start_period": {
              "type": "string",
              "description": "The start date of the period for which data is requested, formatted as 'YYYY-MM-DD', e.g., '2023-01-01'.  Default is '2023-01-01'."
            },
            "end_period": {
              "type": "string",
              "description": "The end date of the period for which data is requested, formatted as 'YYYY-MM-DD', e.g., '2023-12-31'.  Default is '2023-12-31'."
            }
          },
          "required": ["location_ids", "sex", "age", "start_period", "end_period"]
        }
      }
    }
]


def group_population_by_age(data):
    """
    Aggregate the data by grouping entries with the same location, sex, and time period,
    then summarize the ages into an interval and sum the population.

    Args:
        data (list of dict): A list of dictionaries where each dictionary contains
            information about location, sex, age, time period, and population.

    Returns:
        list of dict: A list of dictionaries where the entries have been grouped by
                      location, sex, and time period, with ages aggregated into a range,
                      and populations summed.

    Example:
        data = [
            {'location': 'Bologna', 'sex': 'Total', 'age (years)': '85', 'time period': '2023', 'population': '6759'},
            {'location': 'Bologna', 'sex': 'Total', 'age (years)': '86', 'time period': '2023', 'population': '5892'},
            ...
        ]
        result = aggregate_ages(data)
        # Returns [{'location': 'Bologna', 'sex': 'Total', 'age (years)': '85-100+', 'time period': '2023', 'population': 'sum of all populations'}]
    """
    grouped_data = defaultdict(list)
    # Group data by location, sex, and time period
    for entry in data:
        key = (entry['location'], entry['sex'], entry['time period'])
        grouped_data[key].append(entry)
    result = []
    # Aggregate data for each group
    for (location, sex, time_period), entries in grouped_data.items():
        min_age = float('inf')
        max_age = float('-inf')
        total_population = 0
        for entry in entries:
            # Determine the minimum and maximum ages
            age = entry['age (years)']
            if age == '100+':
                max_age = '100+'
            else:
                age = int(age)
                min_age = min(min_age, age)
                if max_age != '100+':  # Ensure '100+' remains the maximum
                    max_age = max(max_age, age)
            # Sum the population
            total_population += int(entry['population'])
        # Define age range as 'min_age-max_age'
        age_range = f"{min_age}-{max_age}" if max_age == '100+' or min_age != max_age else f"{min_age}"
        # Append the aggregated result
        result.append({
            'location': location,
            'sex': sex,
            'age (years)': age_range,
            'time period': time_period,
            'population': str(total_population)
        })
    return result


def has_multiple_ages(data):
    """
    Check if there are multiple entries for the same combination of location, sex, and time period.

    Args:
        data (list of dict): A list of dictionaries where each dictionary contains
            information about location, sex, age, time period, and population.

    Returns:
        bool: True if there are multiple entries for the same combination of
              location, sex, and time period, otherwise False.
    """
    count_data = defaultdict(int)
    # Count occurrences for each location, sex, and time period combination
    for entry in data:
        key = (entry['location'], entry['sex'], entry['time period'])
        count_data[key] += 1
    # Check if there are any combinations with multiple entries
    return any(count > 1 for count in count_data.values())


def combine_ages(age_code):
    """
    Generate a string representing age codes based on the provided age_code.

    Args:
    age_code (str): The input code representing an age or age group. The code can be one of the following formats:
        - YX: Represents an exact age (e.g., "Y65").
        - Y_GEX: Represents ages X and over up to 100+ (e.g., "Y_GE18").
        - Y_UNX: Represents all ages until X (e.g., "Y_UN18").
        - YX-Z: Represents a range of ages from X to Z (e.g., "Y23-42").

    Returns:
    str: A string representing the sequence of age codes, joined by " + ".

    Rules:
    1. YX -> Return the same string (e.g., "Y65" -> "Y65").
    2. Y_GEX -> Generate a sequence starting from YX to Y_GE100 (e.g., "Y_GE18" -> "Y18 + Y19 + ... + Y_GE100").
    3. Y_UNX -> Generate a sequence from Y0 to Y(X-1) (e.g., "Y_UN18" -> "Y0 + Y1 + ... + Y17").
    4. YX-Z -> Generate a sequence from YX to YZ (e.g., "Y23-42" -> "Y23 + Y24 + ... + Y42").
    """
    if age_code.startswith("Y_"):
        if age_code.startswith("Y_GE"):
            # Rule 1: Y_GEX -> YX + Y(X+1) + ... + Y_GE100
            start_age = int(age_code[4:])
            age_list = [f"Y{age}" for age in range(start_age, 100)]
            age_list.append("Y_GE100")
            return "+".join(age_list)
        elif age_code.startswith("Y_UN"):
            # Rule 2: Y_UNX -> Y0 + Y1 + ... + Y(X-1)
            end_age = int(age_code[4:])
            age_list = [f"Y{age}" for age in range(end_age)]
            return "+".join(age_list)
    elif "-" in age_code:
        # Rule 3: YX-Z -> YX + Y(X+1) + ... + YZ
        start_age, end_age = map(int, age_code[1:].split('-'))
        age_list = [f"Y{age}" for age in range(start_age, end_age + 1)]
        return "+".join(age_list)
    else:
        # Rule 0: YX -> YX
        return age_code



def age_str_to_int(age_str):
    """
    Custom sorting function for age strings.

    Args:
    age_str (str): The age value as a string. This can be a numeric value (e.g., "0", "1", "99")
                   or the special value "100+".

    Returns:
    int: A numeric value used for sorting. For the special case "100+", it returns 101
         to ensure that it is sorted after all other numeric ages.
         For the special case "TOTAL", it returns 102. For numeric values,
         it returns the integer equivalent of the age string.

    Example usage:
    age_key("5") -> 5
    age_key("100+") -> 101
    """
    if age_str == '100+':
        return 101  # Assign a high value so it sorts last
    if age_str.upper() == 'TOTAL':
        return 102
    return int(age_str)  # Convert numeric age strings to integers


def extract_and_format_data_from_xml_for_streamlit_app(xml_content):
    # Parse the XML content
    root = ET.fromstring(xml_content)
    # Define namespaces for the XML structure
    ns = {
        'generic': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic',
        'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
        'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'
    }
    # Create a dictionary from the locations list for faster lookup
    location_dict = {item[next(iter(item))]: next(iter(item)) for item in locations}
    # List to store the data from all series
    extracted_data = []
    # Iterate over each series in the DataSet
    for series in root.findall('.//generic:Series', ns):
        # Extract common series information
        ref_area_code = series.find(".//generic:Value[@id='REF_AREA']", ns).get('value')
        # Get the location name using the location dictionary
        ref_area_name = location_dict.get(ref_area_code,
                                          "Unknown Location")  # Default to "Unknown Location" if not found
        age_code = series.find(".//generic:Value[@id='AGE']", ns).get('value')
        age_description = transform_age_code(age_code)
        sex_code = series.find(".//generic:Value[@id='SEX']", ns).get('value')
        # Map sex codes to descriptive strings
        sex_map = {'1': 'Male', '2': 'Female', '9': 'Total'}
        sex_description = sex_map.get(sex_code, "Unknown Sex")  # Default to "Unknown Sex" if not found
        # Iterate over each observation in the series
        for obs in series.findall('.//generic:Obs', ns):
            time_period = obs.find(".//generic:ObsDimension[@id='TIME_PERIOD']", ns).get('value')
            obs_value = obs.find('.//generic:ObsValue', ns).get('value')
            # Append extracted data to the series data list
            extracted_data.append({
                'location': ref_area_name,
                'sex': sex_description,
                'age (years)': age_description,
                'time period': time_period,
                'population': obs_value
            })
    # Sorting the list of dictionaries by 'time period', 'location', and 'age (years)'
    extracted_data_sorted = sorted(
        extracted_data,
        key=lambda x: (int(x['time period']), x['location'], age_str_to_int(x['age (years)']))
    )
    return extracted_data_sorted


def fetch_population_for_locations_years_sex_age_via_sdmx(location_ids='IT', sex='9', age='TOTAL', start_period='2023-01-01',
                                                     end_period='2023-12-31'):
    """
    Fetches population data for specific locations, time periods, and sex categories using the Istat SDMX web service.

    Args:
        location_ids (str): The geographical identifiers for the locations concatenated by '+' if multiple. Default is 'IT' for Italy.
        sex (str): The sex category for which data is requested. '1' for male, '2' for female, '9' for total. Can be combined with '+'. Default is '9' for total
        age (str): The age in years for which data is requested. From 'Y0' to 'Y99', 'Y_GE100' for 100 years and above, 'TOTAL' for total. Can be combined with '+'. Default is 'TOTAL' for total

        start_period (str): The start date of the period for which data is requested, formatted as 'YYYY-MM-DD'. Default is '2023-01-01'.
        end_period (str): The end date of the period for which data is requested, formatted as 'YYYY-MM-DD'. Default is '2023-12-31'.

    Returns:
        list: A list of dictionaries containing the population data with reference area, time period, and observation value.

    Example of use:
        fetch_population_for_locations_years_sex_age_via_sdmx('ITC+ITE2+ITF14', '9', 'TOTAL', '2023-01-01', '2023-12-31')
        [{'location': 'Nord-ovest', 'sex': 'Total', 'age': 'Total', 'time period': '2023', 'population': '15858626'},
         {'location': 'Umbria', 'sex': 'Total', 'age': 'Total', 'time period': '2023', 'population': '856407'},
         {'location': 'Chieti', 'sex': 'Total', 'age': 'Total', 'time period': '2023', 'population': '372640'}]
    """
    if age.upper() == "TOTAL":
        combined_age = age.upper()
    else:
        combined_age = combine_ages(age)
    url = f"https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289_DF_DCIS_POPRES1_1,1.0/A.{location_ids}.JAN.{sex}.{combined_age}.99/ALL/?detail=full&startPeriod={start_period}&endPeriod={end_period}&dimensionAtObservation=TIME_PERIOD"
    print(url)
    res = query_api(url)
    if res is None:
        return None
    else:
        data = extract_and_format_data_from_xml_for_streamlit_app(res)
        return data


######################################
####### Main process execution #######
######################################
# fetch_parse_and_save_dataflows()
locations = assemble_locations()
dimensions_seen = set() # Initialize set to track seen dimension_ids
useful_datastructures = []
#
# for dataflow_id in useful_dataflow_ids:
#     get_datastructures(dataflow_id)
#     get_constraints(dataflow_id)
# save_as_jsonl(useful_datastructures, "useful_datastructures.jsonl")
# save_CL_ITTER107_codelist_as_jsonl("A", "22_289")  # For geographic areas
# save_CL_ITTER107_codelist_as_jsonl("R", "22_289")  # For regions
# save_CL_ITTER107_codelist_as_jsonl("P", "22_289")  # For provinces
# save_CL_ITTER107_codelist_as_jsonl("M", "22_289")  # For municipalities
#
# save_codelist_as_xml("CL_ITTER107")
# save_codelist_as_jsonl("22_289", "CL_SEXISTAT1")
# save_codelist_as_jsonl("22_289", "CL_ETA1")

