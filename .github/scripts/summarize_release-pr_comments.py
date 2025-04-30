import os
import sys
from github import Github
import openai
from datetime import datetime

# Get environment variables
PR_NUMBER = os.getenv("PR_NUMBER")
REPO_NAME = os.getenv("REPO_NAME")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
VERSION = os.getenv("VERSION")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Template for summarizing PR comments
summarize_template = """
I have comments from multiple PRs that were merged into a release branch. Each comment summarizes changes from a feature PR.

Here are the comments:

{pr_comments}

Based on these comments, create a comprehensive summary of all changes for this release. 
Group them into these categories: Bugs, New Features, Documentation, and Maintenance.

Follow these instructions:
1. Start with a brief overall summary paragraph (2-3 sentences) describing the highlights of this release
2. Then list changes by category using Markdown with level 5 headings (##### Category)
3. Use bullet points for items within each category
4. If a category has no changes, include it with "- None identified"
5. Be concise but informative
6. Improve the writing from the original comments
7. Remove any redundancies or duplicates

Return the summary in this format:

[Overall summary paragraph]

##### Bugs
- [Bug fixes]

##### New features
- [New features]

##### Documentation updates
- [Documentation changes]

##### Maintenance
- [Maintenance changes]
"""


def get_pr_comments(repo_name, pr_number, github_token):
    """Get all comments from a specific PR"""
    if not github_token:
        raise RuntimeError("Please set GITHUB_TOKEN in your environment")

    gh = Github(github_token)
    repo = gh.get_repo(repo_name)
    pr = repo.get_pull(int(pr_number))

    comments = []
    for comment in pr.get_issue_comments():
        # Skip automated comments - adjust this filtering as needed
        # if "This comment was automatically generated" not in comment.body:
        comments.append(comment.body)

    return comments


def summarize_comments(comments, version):
    """Use OpenAI to summarize PR comments"""
    client = openai.OpenAI(api_key=OPENAI_API_KEY)

    # Join all comments with separators
    all_comments = "\n\n---\n\n".join(comments)
    date = datetime.now().strftime("%d-%m-%Y")

    # Call the OpenAI API to summarize
    completion = client.chat.completions.create(
        model="o4-mini-2025-04-16",
        temperature=1.0,
        messages=[
            {
                "role": "system",
                "content": "You are a professional release manager that summarizes changes for software releases.",
            },
            {
                "role": "user",
                "content": summarize_template.format(
                    pr_comments=all_comments, version=version, date=date
                ),
            },
        ],
    )

    summary = completion.choices[0].message.content.strip()
    return summary


def update_changelog(summary):
    """Update the CHANGELOG.md file with the new release summary"""
    try:
        # Check if CHANGELOG.md exists
        with open("CHANGELOG.md", "r") as file:
            existing_content = file.read()
    except FileNotFoundError:
        # Create a new file if it doesn't exist
        existing_content = ""

    # Prepend the summary to the existing content
    if existing_content:
        if not existing_content.startswith("---"):
            new_content = summary + "\n\n---\n\n" + existing_content
        else:
            new_content = summary + "\n\n" + existing_content
    else:
        new_content = summary

    # Write the updated changelog back to the file
    with open("CHANGELOG.md", "w") as file:
        file.write(new_content)

    return True


def main():
    if not all([PR_NUMBER, REPO_NAME, GITHUB_TOKEN, VERSION, OPENAI_API_KEY]):
        missing = []
        if not PR_NUMBER:
            missing.append("PR_NUMBER")
        if not REPO_NAME:
            missing.append("REPO_NAME")
        if not GITHUB_TOKEN:
            missing.append("GITHUB_TOKEN")
        if not VERSION:
            missing.append("VERSION")
        if not OPENAI_API_KEY:
            missing.append("OPENAI_API_KEY")
        print(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)

    # Get comments from the release PR
    comments = get_pr_comments(REPO_NAME, PR_NUMBER, GITHUB_TOKEN)
    date = datetime.now().strftime("%d-%m-%Y")
    if not comments:
        print("No meaningful comments found in the Release PR.")
        # Create a basic summary if no comments are found
        summary = f"""# Release v{VERSION} ({date})

This release includes various improvements and changes.

##### Bugs
- None identified

##### New features
- None identified

##### Documentation updates
- None identified

##### Maintenance
- Repository maintenance updates
"""
    else:
        # Summarize the PR comments
        summary = (
            f"# Release v{VERSION} ({date})\n\n{summarize_comments(comments, VERSION)}"
        )

    # Update the CHANGELOG.md file
    update_changelog(summary)

    # Write the summary to a file for the release notes
    with open("RELEASE_NOTES.md", "w") as file:
        file.write(summary)

    print("✅ Successfully created release notes and updated CHANGELOG.md")


if __name__ == "__main__":
    main()
