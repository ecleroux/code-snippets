# pip install bs4
import requests
from bs4 import BeautifulSoup

all_pages = []

# Creating the results csv file
with open('results.csv', 'w') as file:
    file.write('url,result,message,href\n')  # Writing the header


def crawl_page(url):

    print(f'Validating {url}')

    # Send a GET request to the given URL
    response = requests.get(url)

    # Parse the HTML content of the page using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    # Validate content
    validate_page_content(url, soup)

    # Extract all the anchor tags (<a>) from the page
    links = soup.find_all('a')

    # TODO: get other subpages like the learn articles that are mapped differently

    # Iterate through all the links and find the sub pages
    subpages = []
    for link in links:
        href = link.get('href')

        # Check if the href attribute is not None and if it starts with any of the subpages
        if href is not None:
            # Remove / if it ends with /
            if href.endswith('/'):
                href = href[:-1]

            for subpage in subpages:
                if href.startswith(subpage) and href != subpage and href not in all_pages:
                    subpages.append(href)
                    break

            # If the href didn't start with any existing subpages, check if it starts with the given URL
            # else is hit if we never break out of loop
            else:
                if href.startswith(url) and href != url and href not in all_pages:
                    subpages.append(href)

    # Remove duplicates:
    subpages = list(set(subpages))

    # Add to all_pages
    all_pages.extend(subpages)

    # Recursively crawl the new subpages of the subpages
    for subpage in subpages:
        crawl_page(subpage)


def validate_page_content(url, soup):

    canonical = soup.find("link", {"rel": "canonical"})

    message = ''
    if canonical is None:
        canonical = soup.find("link", {"rel": "Canonical"})
        if canonical:
            message = "Canonical found with upper case 'C'"

    if canonical:
        href = canonical.get("href")
        if url == href:
            result = 'pass'
        else:
            result = 'mismatch'
    else:
        result = 'missing'
        href = ''

    if result == 'missing':
        canonical = soup.find("link", {"rel": "Canonical"})

    # Writing results to the csv file
    with open('results.csv', 'a') as file:
        file.write(f"{url},{result},{message},{href}\n")

    # TODO: Add more validations


crawl_page('https://www.abc.co.za')
