import os
import sys
import requests

from dotenv import load_dotenv

load_dotenv()

# Get environment variables
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
REPO_NAME = os.getenv('REPO_NAME')
RELEASE_ID = os.getenv('RELEASE_ID')  # Pass the release ID from the GitHub Action context

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
}



def get_release_details(release_id):
    api_url = f"https://api.github.com/repos/{REPO_NAME}/releases/{release_id}"
    response = requests.get(api_url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        release_data = response.json()
        # print("Release Data:")
        # print(f"Release ID: {release_data['id']}, Tag Name: {release_data['tag_name']}")
        # print(f"Release Body: {release_data['body']}")
        # print(f"Release URL: {release_data['html_url']}")
    else:
        # print(f"Failed to fetch release data: {response.status_code}")
        # print(response.json())
        release_data={}
    return release_data


if __name__ == "__main__":
    release_data=get_release_details(RELEASE_ID)
    print(release_data['tag_name'])