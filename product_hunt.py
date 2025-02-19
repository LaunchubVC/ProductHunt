import json
import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime, timedelta
import openai

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Ensure these are set in your Lambda environment variables
organization = "org-IYzylArPVqn7mR9hhwTzhT7Y"
api_key = ""
client = openai.OpenAI(api_key="")


CORE_REGION = ['Bulgaria', 'Romania', 'Greece', 'Serbia', 'North Macedonia', 'Macedonia',
               'Slovenia', 'Croatia', 'Moldova', 'Albania', 'Kosovo', 'Bosnia and Herzegovina',
               'Montenegro', 'Hungary', 'Czech Republic', 'Poland']

ACCESS_TOKEN = '';
PRODUCTHUNT_LIST_UUID = 'ecd549d5-58ba-4362-983b-694c5886caa4'
REGIONAL_CS_UUID = '7bed31f9-96d1-451e-92df-219f40c6182f'
OBJECT_COMPANY = '2a252780-2620-46de-87cd-26d7bbd28b50'

class Product:
    def __init__(self, link, company_name, description, long_description, upvotes, application_category, website_url,
                 makers, maker_countries):
        self.link = f"https://www.producthunt.com{link}"
        self.company_name = company_name
        self.description = description
        self.long_description = long_description
        self.upvotes = upvotes
        self.application_category = application_category
        self.website_url = website_url
        self.makers = makers
        self.maker_countries = maker_countries

def resolve_redirect(url):
    try:
        response = requests.get(url, allow_redirects=True)
        return response.url
    except requests.RequestException as e:
        logger.error(f"An error occurred while resolving URL {url}: {e}")
        return url

def fetch_and_process_product_hunt_data(posted_after, posted_before):
    # Define the GraphQL queries with dynamic dates
    query_first = f"""
    query PostRanking {{
      posts(
        postedAfter: "{posted_after}", 
        order: RANKING,
        postedBefore: "{posted_before}"
      ) {{
        nodes {{
            id
            name
            slug
            website
            url
            votesCount
            description
        }}
      }}
    }}
    """

    query_second = f"""
    query PostRanking {{
      posts(
        postedAfter: "{posted_after}", 
        order: RANKING,
        postedBefore: "{posted_before}",
        after: "MjA"
      ) {{
        nodes {{
            id
            name
            slug
            website
            url
            votesCount
            description
        }}
      }}
    }}
    """

    headers = {
        "Authorization": "Bearer X3Uvj-RMiQyCs9JG4udrP8xXYnnNHNE72r2-92jGgj4",
        "Content-Type": "application/json"
    }

    url = "https://api.producthunt.com/v2/api/graphql"

    try:
        response_data_first = requests.post(url, json={"query": query_first}, headers=headers)
        response_data_second = requests.post(url, json={"query": query_second}, headers=headers)
       

        response_data_first.raise_for_status()
        response_data_second.raise_for_status()

        posts_data = response_data_first.json()["data"]["posts"]["nodes"]
        posts_data.extend(response_data_second.json()["data"]["posts"]["nodes"])

        return posts_data
    except requests.RequestException as e:
        logger.error(f"Request to Product Hunt API failed: {e}")
        return {"error": str(e)}
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return {"error": str(e)}

def fetch_makers(slug):
    url = f"https://www.producthunt.com/products/{slug}/makers"
    response = requests.get(url)
    logger.info(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        user_items = soup.find_all('li', class_='styles_makerItem__M0pza mb-6 flex flex-row')
        logger.info(user_items)
        
        users = []
        for item in user_items:
            user_name_elem = item.find('a', class_='text-16 font-bold text-dark-gray')
            if user_name_elem:
                users.append(user_name_elem.text.strip())

        return users
    else:
        logger.error(f"Failed to fetch makers for {slug}: HTTP {response.status_code} using {url}")

        return []

def openai_country_of_origin(name):
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": ''},
                {"role": "user", "content": f"""[Objective]

From a given first name and last name give your best estimate on what's the country of origin of the person. 

[Constraint]

Answer ONLY with the name of the country you're most confident in. Do not give any other answer longer than 3 words that's not a name of a country. 

[Example]

input: John Doe  output: United States
input: Maria Ivanova output: Bulgaria
input: Ivan Blazic output: Croatia

--

The person's name is: {name}

Your best guess:"""}
            ]
        )
        return response.choices[0].message.content
    except Exception as e:
        logger.error(f'Error in openai_country_of_origin: {e}')
        return None

def create_record(domain, name, description):
    url = f"https://api.attio.com/v2/objects/{OBJECT_COMPANY}/records?matching_attribute=domains"
    payload = {"data": {"values": {
                "domains": [domain],
                "name": name,
                "description": description,
    }}}
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Bearer {ACCESS_TOKEN}"
    }
    response = requests.put(url, json=payload, headers=headers)
    json_obj = json.loads(response.text)
    return json_obj["data"]["id"]["record_id"]

def create_new_entry(record_id, short_description, category, makers, country, long_description, upvotes):
    url = f"https://api.attio.com/v2/lists/{PRODUCTHUNT_LIST_UUID}/entries"
    payload = {"data": {
        "parent_record_id": record_id,
        "parent_object": OBJECT_COMPANY,
        "entry_values": {
            "description": short_description, 
            "category": category,
            "makers": ', '.join(makers),
            "country": [country],
            "long_description": long_description,
            'upvotes': upvotes
        }
    }}
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Bearer {ACCESS_TOKEN}"
    }
    response = requests.post(url, json=payload, headers=headers)
    logger.info(f"Attio response: {response.text}")

def add_category(category):
    url = f"https://api.attio.com/v2/lists/{PRODUCTHUNT_LIST_UUID}/attributes/ba6d502d-e716-44bb-949f-a7e01f98f842/options"
    payload = {"data": {"title": category}}
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Bearer {ACCESS_TOKEN}"
    }
    response = requests.post(url, json=payload, headers=headers)
    logger.info(f"Add category response: {response.text}")

def add_country(country):
    url = f"https://api.attio.com/v2/lists/{PRODUCTHUNT_LIST_UUID}/attributes/d428ad72-4a54-4801-849a-27d51addf0a4/options"
    payload = {"data": {"title": country}}
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Bearer {ACCESS_TOKEN}"
    }
    response = requests.post(url, json=payload, headers=headers)
    logger.info(f"Add country response: {response.text}")

def add_a_record_to_producthunt(product):
    website = product.website_url.replace("https://www.", "")
    record_id = create_record(website, product.company_name, product.description)
    add_category(product.application_category)
    add_country(product.maker_countries[0])
    create_new_entry(record_id, product.description, product.application_category, product.makers,
                     product.maker_countries[0], product.long_description, product.upvotes)
    
    # Log the added product
    logger.info(f"Added to Attio: {product.company_name} (ID: {record_id})")
    logger.info(f"  Website: {product.website_url}")
    logger.info(f"  Description: {product.description}")
    logger.info(f"  Category: {product.application_category}")
    logger.info(f"  Makers: {', '.join(product.makers)}")
    logger.info(f"  Countries: {', '.join(product.maker_countries)}")
    logger.info(f"  Upvotes: {product.upvotes}")

def lambda_handler(event, context):
    event_date_str = event['time']
    event_date = datetime.strptime(event_date_str, "%Y-%m-%dT%H:%M:%SZ")

    posted_after = (event_date - timedelta(days=1)).strftime("%a, %d %b %Y %H:%M:%S GMT")
    posted_before = event_date.strftime("%a, %d %b %Y %H:%M:%S GMT")

    logger.info(f"Querying posts from {posted_after} to {posted_before}")

    posts_data = fetch_and_process_product_hunt_data(posted_after, posted_before)

    added_products_count = 0
    logger.info(f"Count of posts_data: {len(posts_data)}")
    p = 0
    for post in posts_data:
        logger.info(f"which in the loop {p}")
        p+=1
        post["website"] = resolve_redirect(post["website"])
        post["makers"] = fetch_makers(post["slug"])
        
        maker_countries = []
        for maker_name in post["makers"]:
            country = openai_country_of_origin(maker_name)
            if country in CORE_REGION:
                maker_countries.append(country)
        
        if maker_countries:
            product = Product(
                link=post["url"],
                company_name=post["name"],
                description=post["description"],
                long_description=post.get("description", ""),
                upvotes=post["votesCount"],
                application_category="No category",
                website_url=post["website"],
                makers=post["makers"],
                maker_countries=maker_countries
            )
            
            add_a_record_to_producthunt(product)
            added_products_count += 1

    logger.info(f"Total products added to Attio: {added_products_count}")

    return {
        'statusCode': 200,
        'body': json.dumps({"message": f"{added_products_count} products added to Attio successfully"})
    }
