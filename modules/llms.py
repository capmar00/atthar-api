from modules.shared import *
from openai import AzureOpenAI


LLMs = {
    "gpt3.5": {
        "api_key": config.get("AZURE_OPENAI_API_KEY_gpt35"),
        "azure_endpoint": config.get("AZURE_OPENAI_ENDPOINT_gpt35"),
        "openai_api_version": config.get("OPENAI_API_VERSION_gpt35"),
        "azure_deployment": config.get("OPENAI_DEPLOYMENT_NAME_gpt35")
    },
    "gpt4o": {
        "api_key": config.get("AZURE_OPENAI_API_KEY_gpt4o"),
        "azure_endpoint": config.get("AZURE_OPENAI_ENDPOINT_gpt4o"),
        "openai_api_version": config.get("OPENAI_API_VERSION_gpt4o"),
        "azure_deployment": config.get("OPENAI_DEPLOYMENT_NAME_gpt4o")
    },
    "gpt4": {
        "api_key": config.get("AZURE_OPENAI_API_KEY_gpt4"),
        "azure_endpoint": config.get("AZURE_OPENAI_ENDPOINT_gpt4"),
        "openai_api_version": config.get("OPENAI_API_VERSION_gpt4"),
        "azure_deployment": config.get("OPENAI_DEPLOYMENT_NAME_gpt4")
    }
}


def initialize_AzureOpenAI_llm(selection):  # istat
    required_keys = ["api_key", "azure_endpoint", "openai_api_version", "azure_deployment"]
    params = LLMs.get(selection, {})
    missing_keys = [key for key in required_keys if key not in params]
    if missing_keys:
        st.error(f"Missing configuration for {', '.join(missing_keys)}. Please check your settings.")
        return None
    return {
        "api_key": params["api_key"],
        "azure_endpoint": params["azure_endpoint"],
        "openai_api_version": params["openai_api_version"],
        "azure_deployment": params["azure_deployment"],
    }


# Function to fetch chat completions from Azure OpenAI
def get_chat_completion(messages, model_config, temperature=0, max_tokens=300, tools=None, tool_choice=None):
    """
    Fetches a completion from Azure OpenAI based on the provided messages and configuration.

    Args:
        messages (list of dict): A list of messages in the chat with roles (system, user, assistant) and their respective content.
        model_config (dict): Configuration of the deployment model, including api_key, endpoint, version, and deployment name.
        temperature (float): Controls randomness in the response generation. Default is 0, indicating deterministic results.
        max_tokens (int): Maximum number of tokens to generate in the response. Default is 300.
        tools (list of str): Optional list of tools that can be enabled if supported by the deployment. Default is None.
        tool_choice (str): Strategy for choosing between enabled tools, defaulting to 'auto' which lets the system decide the best tool to use based on the context.

    Returns:
        str: The content of the response message.
    """
    client = AzureOpenAI(
        api_key=model_config["api_key"],
        azure_endpoint=model_config["azure_endpoint"],
        api_version=model_config["openai_api_version"],
    )
    response = client.chat.completions.create(
        model=model_config["azure_deployment"],
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
        tools=tools,
        tool_choice=tool_choice,
    )
    return response.choices[0].message