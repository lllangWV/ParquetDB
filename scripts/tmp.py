import os
import sys
import requests

from dotenv import load_dotenv

load_dotenv()

# Get environment variables
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
REPO_NAME = os.getenv('GITHUB_REPOSITORY')
RELEASE_ID = os.getenv('RELEASE_ID')  # Pass the release ID from the GitHub Action context

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
}

def get_release_ids():
    

    api_url = f"https://api.github.com/repos/{REPO_NAME}/releases/{RELEASE_ID}"

    response = requests.get(api_url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        release_data = response.json()
        print("Release Data:")
        for release in release_data:
            print(f"Release ID: {release['id']}, Tag Name: {release['tag_name']}")
    else:
        print(f"Failed to fetch release data: {response.status_code}")
        print(response.json())

def get_release_details(release_id):
    api_url = f"https://api.github.com/repos/lllangWV/MatDB/releases/{release_id}"

    response = requests.get(api_url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        release_data = response.json()
        print("Release Data:")
        print(f"Release ID: {release_data['id']}, Tag Name: {release_data['tag_name']}")
        print(f"Release Body: {release_data['body']}")
        print(f"Release URL: {release_data['html_url']}")
    else:
        print(f"Failed to fetch release data: {response.status_code}")
        print(response.json())
        release_data={}
    return release_data



def update_release_body(input_string):
    
    try:
        release_data=get_release_details(RELEASE_ID)
        new_release_body=release_data['body'] + '\n\n' + input_string
    except Exception as e:
        print(f"Error: {e}")
        return None
    
    print(new_release_body)

    api_url = f"https://api.github.com/repos/lllangWV/MatDB/releases/{RELEASE_ID}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
        "Content-Type": "application/json",
    }

    json_data = {
        "body": new_release_body,
    }

    response = requests.patch(api_url, headers=headers, json=json_data)

    # Check if the request was successful
    # Check if the request was successful
    if response.status_code == 200:
        print("Release updated successfully!")
        print(response.json())  # Optional: Print the updated release details
    else:
        print(f"Failed to update release: {response.status_code}")
        print(response.json())




if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_string = sys.argv[1]
        print(f"Received string: {input_string}")
    else:
        input_string=''
        print("No string provided.")

    update_release_body(input_string)
