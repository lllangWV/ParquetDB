import os
import sys
import subprocess

import openai
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

api_key = os.getenv("OPENAI_API_KEY")
TAG = os.getenv('TAG')

def run_git_command(command):
    try:
        result = subprocess.run(['git'] + command.split(), 
                                check=True, 
                                capture_output=True, 
                                text=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")
        return e.stderr
    
def bash_command(command):
    try:
        result = subprocess.run(command.split(), 
                                check=True, 
                                capture_output=True, 
                                text=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")
        return e.stderr



summarize_commit_message_template = """
I have the following commit messages:

{commit_messages}

Classify them into the following categories:
1. Bugs
2. New Features
3. Documentation
4. Maintenance

Then, summarize the changes for each category into a bullet point list.

Follow these instructions:
1. Use Markdown formatting
2. Use 5 "#" to indicate a new category.
3. Do not number the categories.
4. If there are no changes, return "No changes"
5. RETURN ONLY THE SUMMARY IN BULLET POINT FORMAT.

Here is an example:

##### Bugs
- None identified
##### New features
- Added new action to execute python script on push to main
##### Documentation updates
- Updated readme
##### Maintenance
- Moved GitHub actions scripts directory to root
- Added tests
- Changed version

"""

changelog_template = """
___

# {version} ({current_date})

{changes_summary}

___
"""

def summarize_commit_messages(commit_messages):
    client = openai.OpenAI(api_key=api_key)
    # Call the OpenAI API to classify and summarize the changes
    completion = client.chat.completions.create(
    model="gpt-4o-mini",
    max_tokens=500,
    temperature=1.0,
    messages=[
        {"role": "system", 
        "content": "You are a professional programmer that is excellent with github hub repository management." 
        "Your goal is to help the user with tasks\n"},
        {
            "role": "user",
            "content": summarize_commit_message_template.format(commit_messages=commit_messages),
        }
    ]
    )
    changes_summary = completion.choices[0].message.content.strip()
    return changes_summary

def generate_changelog_message():
    # Example usage:
    # bash_command('git fetch --all --tags')
    # current_version = bash_command('git tag -l --sort=v:refname').strip()
    # print(f"Current Version: {current_version}")
    # print(f'git log --pretty=format:"%h-%s" {current_version}..')

    commit_logs_str=bash_command(f'git log --pretty=format:"%h-%s" {TAG}..')
    commit_logs=commit_logs_str.split('\n')
    commit_messages=[commit_log.split('-')[-1] for commit_log in commit_logs]
    # print(commit_messages)

    changes_summary=summarize_commit_messages(commit_messages)
    current_date = datetime.now().strftime("%m-%d-%Y")
    changelog_message=changelog_template.format(version=TAG, 
                                                changes_summary=changes_summary,
                                                current_date=current_date)
    # print('-'*200)
    print(changelog_message)
    return changelog_message

def modify_changelog(changelog_message):

    # Read the current CHANGELOG.md
    try:
        with open('CHANGELOG.md', 'r') as file:
            current_changelog = file.read()
    except FileNotFoundError:
        current_changelog = "___"

    # Prepend the new changelog message
    updated_changelog = changelog_message + current_changelog
    # Write the updated changelog back to the file
    with open('CHANGELOG.md', 'w') as file:
        file.write(updated_changelog)

    # print(os.getcwd())
    # print(os.listdir(os.getcwd()))


if __name__ == "__main__":
    changelog_message=generate_changelog_message()


    modify_changelog(changelog_message)
    
