import httpx
from bs4 import BeautifulSoup
import json
import time
import asyncio
import pandas as pd
from datetime import datetime
import copy

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

p = time.time()
BASE = "https://www.clinique.com"
PRODUCT_CAT_URLS = [
    "https://www.clinique.com/mens",
    "https://www.clinique.com/products/1577/fragrance",
    "https://www.clinique.com/makeup-clinique",
    "https://www.clinique.com/skincare-all",
]
clinique_rating = {
    "collected_on": [],
    "product_name": [],
    "product_cat": [],
    "sku": ['test', 'test', 'test', ],
    "review": [],
    "review_count": [],
    "url": [],
}
reviews_template = {
    "collected_on": [],
    "sku": [],
    "is_staff_reviewer": [],
    "is_verified_buyer": [],
    "is_verified_reviewer": [],
    "helpful_votes": [],
    "not_helpful_votes": [],
    "rating": [],
    "helpful_score": [],
    "comments": [],
    "headline": [],
    "nickname": [],
    # properties
    "age": [],
    "gender": [],
    "incentive": [],
    "skinconcerns": [],
    "skintype": [],
    "cliniquecustomerfor": [],
    "smartrewards2": [],
    # end
    "locale": [],
    "location": [],
    "created_date": [],
    "updated_date": [],
    "bottom_line": [],
    "product_page_id": [],
    "upc": [],
    "gtin": [],
    "merchant_response": [],
    "merchant_response_date": [],
    "disclosure_code": [],
    'url': [],
}
properties = [
    "smartrewards2",
    "age",
    "gender",
    "skinconcerns",
    "skintype",
    "cliniquecustomerfor",
    "incentive",
]
DIRECTORY = "./downloads/"
DIRECTORY = "./exports/"
PRODUCT_CAT = {}
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/109.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "http://www.google.com/",  # this made it work
    "Accept-Encoding": "gzip, deflate, br",  # this made it work
}


class Clinique:
    def get_response(self, client: httpx.Client, url):
        try:
            # TODO: error handling here: json.decoder.JSONDecodeError
            c = client.get(url, headers=HEADERS, timeout=300.0).json()
            return c
        except json.decoder.JSONDecodeError:
            print(f"JSONDecodeError in {url}")

    def process_response(self, response, reviews, sku, url):
        for _, res in enumerate(response):
            reviews["sku"].append(sku)
            reviews["url"].append(url)
            datetime_string = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            reviews["collected_on"].append(datetime_string)
            # processing badges:  {"is_staff_reviewer": false,"is_verified_buyer": false,"is_verified_reviewer": true},
            for k, v in res["badges"].items():
                reviews[k].append(v)
            # processing metrics "metrics": {"helpful_votes": 3,"not_helpful_votes": 0,"rating": 5,"helpful_score": 1835}
            for k, v in res["metrics"].items():
                reviews[k].append(v)
            # processing details: contains comments data and user data
            for k, v in res["details"].items():
                if k == "properties":
                    # TODO: add a way to check list len here
                    collected_keys = []
                    reject = [
                        "fragrancetype",
                        "pros",
                        "cons",
                        "hairtexture",
                        "describeyourself",
                        "bestuses",
                        "brand_base_url",
                        "wasthisagift", 
                    ]
                    for seg in v:
                        if seg["key"] in reject:
                            continue
                        if seg["key"] == "wasthisreviewedaspartofasweepstakesorcontest":
                            reviews["incentive"].append(str(seg["value"][0]))
                            collected_keys.append("incentive")
                        else:
                            if len(seg["value"]) == 1:
                                reviews[seg["key"]].append(str(seg["value"][0]))
                                collected_keys.append(seg["key"])
                            else:
                                reviews[seg["key"]].append(", ".join(seg["value"]))
                                collected_keys.append(seg["key"])
                    for p in properties:
                        if p not in collected_keys:
                            reviews[p].append("")
                else:
                    if (
                        k == "created_date"
                        or k == "updated_date"
                        or k == "merchant_response_date"
                    ):
                        time_stamp = datetime.fromtimestamp((v / 1000))
                        time_stamp = time_stamp.strftime("%Y-%m-%d %H:%M:%S")
                        reviews[k].append(time_stamp)
                    else:
                        if k in reviews:
                            reviews[k].append(v)
            for m in ["merchant_response", "merchant_response_date", "disclosure_code"]:
                if m not in res["details"]:
                    reviews[m].append("")
            for k, v in reviews.items():
                base = len(reviews["sku"])
                if base != len(v):
                    reviews[k].append("")
        return reviews

    def site_map(self, export=0):
        """
        Scrapes all products URLs from Clinique and return a dict with all product links by categories
        """
        headers = [
            "product_impression_name",
            "product_impression_url",
            "product_impression_sku",
            "product_impression_product_code",
            "product_impression_id",
            "product_impression_category",
            "product_impression_base_id",
        ]
        categories = [
            "men",
            "fragrance",
            "makeup",
            "skincare",
        ]
        products_data = dict(zip(categories, [{} for _ in categories]))
        p = 0
        with httpx.Client() as client:
            for i, url in enumerate(PRODUCT_CAT_URLS):
                product_data = dict(zip(headers, [[] for _ in headers]))
                response = client.get(url)
                soup = BeautifulSoup(response.text, "html.parser")
                scripts = soup.find_all("script")
                data_str = (
                    scripts[-2].get_text().strip().replace("window.page_data = ", "")
                )
                data = json.loads(data_str)
                for k, v in data["analytics-datalayer"].items():
                    if k in headers:
                        if k == 'product_impression_url': 
                            urls = []
                            for url in v: 
                                urls.append((BASE + str(url)))
                            product_data[k] = urls
                            continue
                        product_data[k] = v
                products_data[categories[i]] = product_data
        return products_data

    def scrape_reviews(self, urls):
        base_url = "https://display.powerreviews.com/m/"
        api_key = "apikey=528023b7-ebfb-4f03-8fee-2282777437a7&_noconfig=true"
        requests_count = 0
        skipped = []
        clinique_reviews = copy.deepcopy(reviews_template)
        product_ids = []
        skus = clinique_rating["sku"]
        # urls = clinique_rating["url"]

        with httpx.Client(
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=50)
        ) as client:
            for i, url in enumerate(urls):
                product_id = int(
                    str(url).strip("https://www.clinique.com/").split("/")[2]
                )
                product_url = f"166973/l/en_US/product/{product_id}/reviews?"
                data_url = base_url + product_url + api_key
                # print(data_url)
                if product_id in product_ids:
                    skipped.append(url)
                    print(f"Skipped: {i, url}, already in list")
                    continue

                response = self.get_response(client, data_url)
                requests_count += 1
                num_results = response["paging"]["total_results"]
                page_size = response["paging"]["page_size"]
                response = response["results"][0]["reviews"]
                reviews_length = len(response)
                current_reviews = self.process_response(
                    response, clinique_reviews, skus[i], url
                )

                while True:
                    next_url = f"166973/l/en_US/product/{product_id}/reviews?apikey=528023b7-ebfb-4f03-8fee-2282777437a7&_noconfig=true&paging.from={page_size}&paging.size=25"
                    response = self.get_response(client, (base_url + next_url))[
                        "results"
                    ][0]["reviews"]
                    current_reviews = self.process_response(
                        response, clinique_reviews, skus[i], url
                    )
                    reviews_length += len(response)
                    page_size = reviews_length
                    requests_count += 1

                    if reviews_length >= num_results:
                        break
                product_ids.append(product_id)
                print(
                    f"Reviews Scraping done: {num_results}, Progress: {len(product_ids)}/{len(urls)}"
                )
        with open(DIRECTORY + "clinique_reviews.json", "w") as f:
            json.dump(clinique_reviews, f)
        df = pd.DataFrame(clinique_reviews)
        df.to_excel(DIRECTORY + "clinique_reviews.xlsx", index=False)

        return clinique_reviews

    async def get_page(self, client: httpx.Client, url, i, PRODUCT_CAT):
        try:
            response = await client.get(url)
            soup = BeautifulSoup(response.text, "html.parser")
            js = soup.find("script", {"type": "application/ld+json"})
            js = json.loads(js.get_text())
            clinique_rating["review"].append(
                float(js["aggregateRating"]["ratingValue"])
            )
            clinique_rating["review_count"].append(
                int(js["aggregateRating"]["reviewCount"])
            )
        except AttributeError:
            print(f"Attribute Error Failed: {url}")
            return {"attributeError": url}
        except KeyError:
            fail = {js["name"]: url}
            print(f"Key Error Failed: {fail}")
            return fail
        except Exception:
            fail = {"url": url}
            print(f"ssl.SSLError: {fail}")
            return fail

        clinique_rating["product_name"].append(
            str(js["name"]).replace("\u2122", "").replace("&trade;", "")
        )
        clinique_rating["url"].append(url)
        datetime_string = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        clinique_rating["collected_on"].append(datetime_string)
        clinique_rating["sku"].append(js["sku"])
        clinique_rating["product_cat"].append(PRODUCT_CAT[url])
        print(f'{i}) Product Done: {js["name"]}')

    async def scrape_rating(self, reviews, export=0, limit=-1):
        products_data = self.site_map(export)
        urls = [
            z 
            for v in products_data.values()
            for z in v['product_impression_url'] 
        ]
        PRODUCT_CAT = {
            z: k
            for k, v in products_data.items()
            for z in v["product_impression_url"]
        }
        if limit > 0:
            urls = urls[:limit]
        async with httpx.AsyncClient(
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=50)
        ) as client:
            tasks = []
            print("---------> Started scraping products <---------")
            for i, url in enumerate(urls):
                tasks.append(
                    asyncio.create_task(self.get_page(client, url, i, PRODUCT_CAT))
                )
            failed = await asyncio.gather(*tasks)

            print("---------> Scraping Complete products <---------")
            if export:
                with open(DIRECTORY + "clinique_failed.json", "w") as f:
                    json.dump(failed, f) # failed url happens randomly and re-scraping works to solve this issue
                df = pd.DataFrame(reviews)
                df.to_excel(DIRECTORY + "clinique_rating.xlsx", index=False)
            return reviews, failed, urls

    def run(self, reviews=True, export=0, limit=-1):
        rating, failed, urls = asyncio.run(
            self.scrape_rating(clinique_rating, export=export, limit=limit)
        )
        if reviews:
            clinique_reviews = self.scrape_reviews(clinique_rating["url"])
        else:
            clinique_reviews = {}
        return rating, failed, clinique_reviews


if __name__ == "__main__":
    clinique = Clinique()
    # clinique.run(reviews=True, export=1)
    urls = [
        'https://www.clinique.com/product/1687/4977/skincare/moisturizers/acne-solutionstm-all-over-clearing-treatment', 
        'https://www.clinique.com/product/25356/6152/skincare/lip-care/repairweartm-intensive-lip-treatment', 
        'https://www.clinique.com/product/16320/109960/skincare/clinique-smart/clinique-smart-clinical-repairtm-wrinkle-correcting-rich-cream', 
    ]
    clinique.scrape_reviews(urls)
    print(f"Duration: {round((time.time() - p), 3)}s") # runtime ratings = 30s reviews = 760s 
