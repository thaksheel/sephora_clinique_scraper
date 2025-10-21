import httpx
from bs4 import BeautifulSoup
import json
import time
import asyncio
import pandas as pd
from datetime import datetime
import copy
import numpy as np
from thefuzz import fuzz
import math 
import chardet

# Constants
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
p = time.time()


class Clinique:
    def __init__(self, options):
        self.options = options

    def get_response(self, client: httpx.Client, url):
        try:
            # TODO: error handling here: json.decoder.JSONDecodeError
            c = client.get(url, headers=self.options["headers"], timeout=300.0).json()
            return c
        except json.decoder.JSONDecodeError:
            print(f"JSONDecodeError in {url}")

    def process_reviews(self, response, reviews, sku, url):
        for _, res in enumerate(response):
            reviews["sku"].append(sku)
            reviews["url"].append(url)
            reviews["collected_on"].append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
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
                    for p in self.options["properties"]:
                        if p not in collected_keys:
                            reviews[p].append("")
                else:
                    if (
                        k == "created_date"
                        or k == "updated_date"
                        or k == "merchant_response_date"
                    ):
                        time_stamp = datetime.fromtimestamp((v / 1000)).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
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

    def site_map(self):
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
        cat_urls = [
            "https://www.clinique.com/mens",
            "https://www.clinique.com/products/1577/fragrance",
            "https://www.clinique.com/makeup-clinique",
            "https://www.clinique.com/skincare-all",
        ]
        products_data = dict(zip(categories, [{} for _ in categories]))
        with httpx.Client() as client:
            for i, url in enumerate(cat_urls):
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
                        if k == "product_impression_url":
                            urls = []
                            for url in v:
                                urls.append(("https://www.clinique.com" + str(url)))
                            product_data[k] = urls
                            continue
                        product_data[k] = v
                products_data[categories[i]] = product_data
        return products_data

    def scrape_reviews(self, urls, skus):
        requests_count = 0
        skipped = []
        clinique_reviews = copy.deepcopy(self.options["clinique_reviews_template"])
        product_ids = []

        with httpx.Client(
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=50)
        ) as client:
            for i, url in enumerate(urls):
                product_id = int(
                    str(url).strip("https://www.clinique.com/").split("/")[2]
                )
                product_url = f"166973/l/en_US/product/{product_id}/reviews?"
                data_url = (
                    self.options["api_url"] + product_url + self.options["api_key"]
                )
                if product_id in product_ids:
                    skipped.append(url)
                    print(f"Skipped: {i, url}, already in list")
                    continue

                response = self.get_response(client, data_url)
                if response is None: continue
                requests_count += 1
                num_results = response["paging"]["total_results"]
                page_size = response["paging"]["page_size"]
                response = response["results"][0]["reviews"]
                reviews_length = len(response)
                _ = self.process_reviews(response, clinique_reviews, skus[i], url)

                while True:
                    next_url = f"166973/l/en_US/product/{product_id}/reviews?apikey=528023b7-ebfb-4f03-8fee-2282777437a7&_noconfig=true&paging.from={page_size}&paging.size=25"
                    response = self.get_response(
                        client, (self.options["api_url"] + next_url)
                    )["results"][0]["reviews"]
                    _ = self.process_reviews(response, clinique_reviews, skus[i], url)
                    reviews_length += len(response)
                    page_size = reviews_length
                    requests_count += 1
                    if reviews_length >= num_results:
                        break
                product_ids.append(product_id)

                if i % 20 == 0:
                    print(
                        f"Reviews Scraping done: {num_results}, Progress: {len(product_ids)}/{len(urls)}"
                    )
        # with open(options['dir'] + options['filename'] + "_reviews.json", "w") as f:
        #     json.dump(clinique_reviews, f)

        return clinique_reviews

    async def get_page(self, ratings, client: httpx.Client, url, product_cat):
        try:
            response = await client.get(url)
            soup = BeautifulSoup(response.text, "html.parser")
            js = soup.find("script", {"type": "application/ld+json"})
            js = json.loads(js.get_text())
            ratings["review"].append(float(js["aggregateRating"]["ratingValue"]))
            ratings["review_count"].append(int(js["aggregateRating"]["reviewCount"]))
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

        ratings["product_name"].append(
            str(js["name"]).replace("\u2122", "").replace("&trade;", "")
        )
        ratings["url"].append(url)
        ratings["product_ids"].append(
            int(str(url).strip("https://www.clinique.com/").split("/")[2])
        )
        datetime_string = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ratings["collected_on"].append(datetime_string)
        ratings["sku"].append(js["sku"])
        if product_cat:
            ratings["product_cat"].append(product_cat[url])
        else:
            ratings["product_cat"].append(np.nan)

        # print(f'{i}) Product Done: {js["name"]}')

    async def scrape_rating(self, urls=[]):
        ratings = copy.deepcopy(self.options["clinique_ratings_template"])
        if not urls:
            products_data = self.site_map()
            urls = [
                z for v in products_data.values() for z in v["product_impression_url"]
            ]
            product_cat = {
                z: k
                for k, v in products_data.items()
                for z in v["product_impression_url"]
            }
        else:
            product_cat = []
        async with httpx.AsyncClient(
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=50)
        ) as client:
            tasks = []
            print("---------> Started: Clinique Rating <---------")
            for i, url in enumerate(urls):
                tasks.append(
                    asyncio.create_task(
                        self.get_page(ratings, client, url, product_cat)
                    )
                )
            failed = await asyncio.gather(*tasks)
            print("---------> Ended: Clinique Rating <---------")

            return ratings, failed

    def run(self, urls=[], sr=True):
        ratings, failed = asyncio.run(self.scrape_rating(urls))
        file = options["dir"] + options["filename_c"] + "_failed.json"
        with open(file, "w") as f:
            json.dump(
                failed, f
            )  # failed url happens randomly and re-scraping works to solve this issue
        df = pd.DataFrame(ratings)
        df = df.drop_duplicates()
        df.to_csv(options["dir"] + options["filename_c"] + "_rating.csv", index=False)

        if sr:
            reviews = self.scrape_reviews(ratings["url"], ratings["sku"])
            df = pd.DataFrame(reviews)
            df = df.drop_duplicates()
            df.to_csv(
                options["dir"] + options["filename_c"] + "_reviews.csv", index=False
            )
        else:
            reviews = {}
        self.options["clinique_scrap_runtime"] = time.time() - p
        return ratings, failed, reviews


def map_dataset(clinique, sephora, options):
    def _linked(d, clinique, sephora_df, sim_index, sim_ratio):
        d["clinique_sku"].append(str(clinique["sku"]))
        d["clinique_name"].append(str(clinique["product_name"]))
        d["clinique_url"].append(str(clinique["url"]))

        d["sephora_sku"].append(str(sephora_df.loc[sim_index, "sku"]))
        d["sephora_url"].append(str(sephora_df.loc[sim_index, "url"]))
        d["sephora_name"].append(str(sephora_df.loc[sim_index, "product_name"]))
        d["fuzzy_ratio"].append(int(sim_ratio))
        return True

    clinique_df = pd.DataFrame(clinique)
    sephora_df = pd.DataFrame(sephora)
    linked = options["linked"]
    unlinked = options["linked"]
    for _, clinique in clinique_df.iterrows():
        sim_ratios = []
        sim_partial_ratios = []
        for _, sephora in sephora_df.iterrows():
            # TODO: implement the other level of fuzzy strings since something like Clinique For Men Charcoal Face Wash was not present in linked table because of the first 3 words not being present
            ratio = fuzz.ratio(
                str(clinique["product_name"]).strip().lower(),
                str(sephora["product_name"]).strip().lower(),
            )
            partial_ratio = fuzz.partial_ratio(
                str(clinique["product_name"]).strip().lower(),
                str(sephora["product_name"]).strip().lower(),
            )
            sim_ratios.append(ratio)
            sim_partial_ratios.append(partial_ratio)

        sim_ratio = max(sim_ratios)
        sim_index = sim_ratios.index(sim_ratio)
        if sim_ratio > 72:
            if sephora_df.loc[sim_index, "sku"] in options["linked"]["sephora_sku"]:
                index = options["linked"]["sephora_sku"].index(
                    sephora_df.loc[sim_index, "sku"]
                )
                if sim_ratio > options["linked"]["fuzzy_ratio"][index]:
                    _linked(linked, clinique, sephora_df, sim_index, sim_ratio)
            elif (
                sephora_df.loc[sim_index, "sku"] not in options["linked"]["sephora_sku"]
            ):
                _linked(linked, clinique, sephora_df, sim_index, sim_ratio)
        else:
            # TODO: add additional filtering for partial_ratio
            _linked(unlinked, clinique, sephora_df, sim_index, sim_ratio)
        df = pd.DataFrame(linked)
        df = df.drop_duplicates()
        df_unlinked = pd.DataFrame(unlinked)
        df_unlinked = df_unlinked.drop_duplicates()
        df.to_csv(options["dir"] + "map_table.csv", index=False)
        df_unlinked.to_csv(options["dir"] + "unmap_table.csv", index=False)
    return linked, unlinked


class Sephora: 
    def __init__(self, options):
        self.options = options

    def get_pages_num(self, soup: BeautifulSoup):
        try:
            num_products = int(
                soup.find("p", {"data-at": "number_of_products"}).get_text().split()[0]
            )
        except AttributeError:
            num_products = 124
        num_pages = math.ceil(num_products / 60)

        return num_pages, num_products

    def process_response(self, response, reviews, sku, url, product_id):
        for res in response:
            reviews["sku"].append(sku)
            current_time = time.localtime()
            current_time_str = time.strftime("%Y-%m-%d %H:%M:%S", current_time)
            reviews["collected_on"].append(current_time_str)
            reviews["product_id"].append(product_id)
            reviews["urls"].append(url)
            for key, value in res.items():
                if key == "BadgesOrder":
                    reviews["verifiedPurchaser"].append("verifiedPurchaser" in value)
                    reviews["incentivizedReview"].append("incentivizedReview" in value)
                if key in reviews or key == "ContextDataValues":
                    if key == "ContextDataValues":
                        for k, v in res[key].items():
                            if k == "beautyInsider":
                                continue
                            if k in [
                                "skinTone",
                                "hairColor",
                                "IncentivizedReview",
                                "skinType",
                                "StaffContext",
                                "eyeColor",
                            ]:
                                reviews[k].append(v["Value"])
                            else:
                                reviews[k].append("")
                    else:
                        reviews[key].append(value)
            for k, v in reviews.items():
                base = len(reviews["sku"])
                if base != len(v):
                    reviews[k].append("")
        return reviews

    def get_response(self, client: httpx.Client, url):
        return client.get(url).json()

    def scrape_reviews(self, rating):
        with httpx.Client() as client:
            offset = 0
            requests_count = 0
            skus = rating["sku"]
            product_ids = rating["product_id"]
            urls = rating["url"]
            sephora_reviews = copy.deepcopy(self.options['sephora_reviews_template'])
            print("-----------> Started: Sephora Reviews <-----------")

            for i, product_id in enumerate(product_ids):
                data_url = f"https://api.bazaarvoice.com/data/reviews.json?Filter=contentlocale%3Aen*&Filter=ProductId%3A{product_id}&Sort=SubmissionTime%3Adesc&Limit={100}&Offset={offset}&Include=Products%2CComments&Stats=Reviews&passkey=calXm2DyQVjcCy9agq85vmTJv5ELuuBCF2sdg4BnJzJus&apiversion=5.4"
                response = self.get_response(client, data_url)
                requests_count += 1
                num_results = response["TotalResults"]
                response = response["Results"]
                reviews_length = len(response)
                _ = self.process_response(
                    response, sephora_reviews, skus[i], urls[i], product_id
                )

                while True:
                    data_url = f"https://api.bazaarvoice.com/data/reviews.json?Filter=contentlocale%3Aen*&Filter=ProductId%3A{product_id}&Sort=SubmissionTime%3Adesc&Limit={100}&Offset={offset}&Include=Products%2CComments&Stats=Reviews&passkey=calXm2DyQVjcCy9agq85vmTJv5ELuuBCF2sdg4BnJzJus&apiversion=5.4"
                    response = self.get_response(client, data_url)["Results"]
                    _ = self.process_response(
                        response, sephora_reviews, skus[i], urls[i], product_id
                    )
                    reviews_length += len(response)
                    offset = reviews_length
                    requests_count += 1
                    if reviews_length >= num_results:
                        break
        print("-----------> Ended: Sephora Reviews <-----------")
        df = pd.DataFrame(sephora_reviews)
        df = df.drop_duplicates()
        df.to_csv(self.options['dir'] + "sephora_reviews.csv", index=False)

        return sephora_reviews

    def scrape_rating(self, export=0):
        print("---------> Started: Sephora Rating <---------")
        page = httpx.get(self.options['sephora_url'], headers=self.options['headers'], timeout=300.0)
        soup = BeautifulSoup(page.text, "html.parser")
        self.options["num_pages"], num_products = self.get_pages_num(soup)
        sephora_rating = copy.deepcopy(self.options['sephora_rating_template'])

        with httpx.Client(
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=50)
        ) as client:
            for k in range(self.options["num_pages"]):
                QUERY = "?currentPage="
                new_url = self.options['sephora_url'] + QUERY + str(k + 1)
                page = client.get(new_url, headers=self.options['headers'])
                # encoding = chardet.detect(page.content)["encoding"]
                # page = page.content.decode(encoding)
                soup = BeautifulSoup(page.text, "html.parser")
                products_data = json.loads(
                    soup.find("script", {"id": "linkStore"}).get_text()
                )
                products = products_data["page"]["nthBrand"]["products"]
                for i, product in enumerate(products):
                    if product["displayName"] in sephora_rating["product_name"]:
                        continue
                    else:
                        sephora_rating["review"].append(float(product["rating"]))
                        sephora_rating["review_count"].append(int(product["reviews"]))
                        sephora_rating["sku"].append(product["currentSku"]["skuId"])
                        sephora_rating["product_id"].append(product["productId"])
                        sephora_rating["product_name"].append(
                            str(product["displayName"])
                            .replace("\u2122", "")
                            .replace("&trade;", "")
                        )
                        sephora_rating["url"].append(self.options['sephora_base'] + product["targetUrl"])
                        current_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        sephora_rating["collected_on"].append(current_time_str)
                print(
                    f'Progress ({round(len(sephora_rating["product_id"])/num_products, 2) * 100}%): {len(sephora_rating["product_id"])}/{num_products}'
                )
                new_url = self.options['sephora_url'] + self.options['query'] + str(k + 1)
                page = client.get(new_url, headers=self.options['headers'])
                soup = BeautifulSoup(page.text, "html.parser")
        print("---------> End: Sephora Rating <---------")
        if export:
            df = pd.DataFrame(sephora_rating)
            df = df.drop_duplicates()
            df.to_csv(self.options['dir'] + "sephora_rating.csv", index=False)
            print(
                f'Sephore scraping done ({round(len(sephora_rating["product_id"])/num_products, 2) * 100}%): {len(sephora_rating["product_id"])}/{num_products}'
            )
        return sephora_rating

options = {
    "clinique_scrap_runtime": 0,
    "dir": "./exports/",
    "filename_c": "clinique0",
    "headers": {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/109.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer": "http://www.google.com/",  # this made it work
        "Accept-Encoding": "gzip, deflate, br",  # this made it work
    },
    "api_url": "https://display.powerreviews.com/m/",
    "api_key": "apikey=528023b7-ebfb-4f03-8fee-2282777437a7&_noconfig=true",
    "clinique_reviews_template": {
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
        "url": [],
    },
    "properties": [
        "smartrewards2",
        "age",
        "gender",
        "skinconcerns",
        "skintype",
        "cliniquecustomerfor",
        "incentive",
    ],
    "clinique_ratings_template": {
        "collected_on": [],
        "product_ids": [],
        "sku": [],
        "product_name": [],
        "product_cat": [],
        "review": [],
        "review_count": [],
        "url": [],
    },
    "linked": {
        "clinique_sku": [],
        "sephora_sku": [],
        "fuzzy_ratio": [],
        "clinique_name": [],
        "sephora_name": [],
        "sephora_url": [],
        "clinique_url": [],
    },
    "sephora_reviews_template": {
        "collected_on": [],
        "sku": [],
        "product_id": [],
        "incentivizedReview": [],
        "verifiedPurchaser": [],
        "LastModificationTime": [],
        "OriginalProductName": [],
        "IsFeatured": [],
        "TotalCommentCount": [],
        "TotalClientResponseCount": [],
        "TotalInappropriateFeedbackCount": [],
        "Rating": [],
        "IsRatingsOnly": [],
        "IsRecommended": [],
        "TotalPositiveFeedbackCount": [],
        "TotalNegativeFeedbackCount": [],
        "TotalFeedbackCount": [],
        "ModerationStatus": [],
        "SubmissionTime": [],
        "ReviewText": [],
        "Title": [],
        "UserNickname": [],
        "ReviewText": [],
        "UserLocation": [],
        "Helpfulness": [],
        "IsSyndicated": [],
        "age": [],
        "hairCondition": [],
        # ContextDataValues
        "skinType": [],
        "skinTone": [],
        "IncentivizedReview": [],
        "hairColor": [],
        "eyeColor": [],
        "StaffContext": [],
        "urls": [],
    },
    "query": "?currentPage=",
    "sephora_rating_template": {
        "collected_on": [],
        "product_name": [],
        "review": [],
        "review_count": [],
        "url": [],
        "sku": [],
        "product_id": [],
    },
    "sephora_url": "https://www.sephora.com/brand/clinique",
    "sephora_base": "https://www.sephora.com",
    "num_pages": 3, 
}

if __name__ == "__main__":
    clinique = Clinique(options)
    clinique_ratings, failed, reviews = clinique.run()
    
    sephora = Sephora(options)
    sephora_ratings = sephora.scrape_rating(export=1)
    sephora_reviews = sephora.scrape_reviews(sephora_ratings)

    mapped, unmapped = map_dataset(clinique_ratings, sephora_ratings, options)
    print(
        f"Total Runtime: {round((time.time() - p), 3)}s"
    )  # runtime ratings = 30s reviews = 760s
