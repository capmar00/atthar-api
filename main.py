from typing import Union
from fastapi import FastAPI
from modules.llms import *
from modules.utils import *

_geographic_areas = read_jsonl_file("ITTER107/_geographic_areas.jsonl")
_regions = read_jsonl_file("ITTER107/_regions.jsonl")
_provinces = read_jsonl_file("ITTER107/_provinces.jsonl")

llm_name = "gpt4"
llm = initialize_AzureOpenAI_llm(llm_name)
app = FastAPI()

fastapi_response = {
    "title": "Census Data",
    "description": "A table from census data comparing geography with a population-based statistic.",
    "type": "object",
    "requestDuration": 0,
    "requestTokens": 0,
    "data": [],
    "dataURL": "",
    "dataSource": "Istat",
    "analysis": ""
}


@app.post("/")
async def generate_response(prompt):
    messages = []
    messages.append({"role": "user", "content": prompt})

    system_location_ids_prompt = f"""
    "From the provided list of locations, select the one that best matches the user's needs. 
    Geographic areas:
    {_geographic_areas}; 
    Regions:
    {_regions}; 
    Provinces:
    {_provinces}. 

    Instruction:
    Review the user prompts and the locations list, then return the id of the most relevant location without any extra text — just the id, nothing else.
    If they are mupliple locations, combine them with a plus '+' sign.

    Examples:
    - Query: "Tell me the population of Sicilia" -> Response: "ITD3".
    - Query: "I want the unemployment rate in Sud Italia?" -> "ITF".
    - Query: "What is the population of Bologna, Ravenna and Parma?" -> "ITD55+ITD57+ITD52".

    Important: Only return the exact string (e.g., "ITC41") without any additional words or explanations.
    """

    messages.append({"role": "system", "content": system_location_ids_prompt})
    response = get_chat_completion(messages, llm)
    messages.append({"role": "assistant", "content": response.content})
    print("\n\nFIRST response - location id: ", response.content)
    response = get_chat_completion(messages, llm, tools=tools, tool_choice="auto")
    messages.append({"role": "assistant", "content": response})
    tool_call = response.tool_calls[0]
    params = json.loads(tool_call.function.arguments)
    chosen_function = eval(tool_call.function.name)
    final_data = chosen_function(**params)
    if final_data is None:
        info_msg = "OOOPS! Your query returned no results. Try rephrasing your request with more detail."
        messages.append({"role": "assistant", "content": info_msg})
        return info_msg
    else:
        messages.append({"role": "assistant", "content": final_data})
        print("\n\nTHIRD response - chosen function: ", response)
        print("\n\nfn name", tool_call.function.name)
        print("\n\nparams", params)
    print("\n\nfinal data", final_data)
    location_ids_list = params['location_ids'].split('+')
    for index, elem in enumerate(final_data):
        elem_dict = {
            "name": elem['location'],
            "geoID": location_ids_list[index],
            "groupID": "CL_ETA1",
            "groupLabel": "Age class",
            "unit": "individuals",
            "categories": [
                {
                    "variableID": "TOTAL",
                    "variableLabel": "Total Population",
                    "value": int(elem['population'])
                }
            ]
        }
        fastapi_response["data"].append(elem_dict)
    return fastapi_response

