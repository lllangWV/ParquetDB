import os
import requests

# Your GitHub token
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
REPO_NAME = os.getenv('GITHUB_REPOSITORY')
RELEASE_ID = os.getenv('RELEASE_ID')  # Pass the release ID from the GitHub Action context


# GitHub API base URL for releases
api_base_url = f"https://api.github.com/repos/{REPO_NAME}"

# Headers for authentication
headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
}

# Step 1: Get the release ID associated with the tag
def get_release_details(release_id):
    api_url = f"https://api.github.com/repos/{REPO_NAME}/releases/{release_id}"

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

# Step 2: Delete the release using the release ID
def delete_release(release_id):
    if release_id:
        api_url = f"{api_base_url}/releases/{release_id}"
        response = requests.delete(api_url, headers=headers)
        if response.status_code == 204:
            print(f"Release with ID {release_id} deleted successfully.")
        else:
            print(f"Failed to delete release: {response.status_code}")
            print(response.json())

# Step 3: Optionally delete the tag associated with the release
def delete_tag(tag_name):
    api_url = f"{api_base_url}/git/refs/tags/{tag_name}"
    response = requests.delete(api_url, headers=headers)
    if response.status_code == 204:
        print(f"Tag {tag_name} deleted successfully.")
    else:
        print(f"Failed to delete tag {tag_name}: {response.status_code}")
        print(response.json())

# Main logic
if __name__ == "__main__":
    release_data=get_release_details(RELEASE_ID)

    # Delete the release
    delete_release(RELEASE_ID)
    
    # Optionally delete the tag
    delete_tag(release_data['tag_name'])
