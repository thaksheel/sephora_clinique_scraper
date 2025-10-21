from bs4 import BeautifulSoup
import pandas as pd
import json
import httpx
import math
import chardet
import copy
import time


BASE = "https://www.sephora.com"
SEPHORA_URL = "https://www.sephora.com/brand/clinique"
sephora_rating = {
    'collected_on': [], 
    "product_name": [],
    "review": [],
    "review_count": [],
    "url": [],
    "sku": [],
    "product_id": [],
}
DATA = {
    "num_pages": 3,
}
QUERY = "?currentPage="
DIRECTORY = "./downloads/"
DIRECTORY = "./exports/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/109.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "http://www.google.com/",  # this made it work
    # "Accept-Encoding": "gzip, deflate, br",  # this makes it not work
}
reviews_template = {
    'collected_on': [], 
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
}
p = time.time()


class Sephora:
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

    def scrape_reviews(self):
        with httpx.Client() as client:
            offset = 0
            requests_count = 0
            skus = sephora_rating["sku"]
            product_ids = sephora_rating["product_id"]
            urls = sephora_rating["url"]
            sephora_reviews = copy.deepcopy(reviews_template)
            print("Started Sephora Reviews Scraping")

            for i, product_id in enumerate(product_ids):
                data_url = f"https://api.bazaarvoice.com/data/reviews.json?Filter=contentlocale%3Aen*&Filter=ProductId%3A{product_id}&Sort=SubmissionTime%3Adesc&Limit={100}&Offset={offset}&Include=Products%2CComments&Stats=Reviews&passkey=calXm2DyQVjcCy9agq85vmTJv5ELuuBCF2sdg4BnJzJus&apiversion=5.4"
                response = self.get_response(client, data_url)
                requests_count += 1
                num_results = response["TotalResults"]
                response = response["Results"]
                reviews_length = len(response)
                current_reviews = self.process_response(
                    response, sephora_reviews, skus[i], urls[i], product_id
                )

                while True:
                    data_url = f"https://api.bazaarvoice.com/data/reviews.json?Filter=contentlocale%3Aen*&Filter=ProductId%3A{product_id}&Sort=SubmissionTime%3Adesc&Limit={100}&Offset={offset}&Include=Products%2CComments&Stats=Reviews&passkey=calXm2DyQVjcCy9agq85vmTJv5ELuuBCF2sdg4BnJzJus&apiversion=5.4"
                    response = self.get_response(client, data_url)["Results"]
                    current_reviews = self.process_response(
                        response, sephora_reviews, skus[i], urls[i], product_id
                    )
                    reviews_length += len(response)
                    offset = reviews_length
                    requests_count += 1
                    if reviews_length >= num_results:
                        break
                print(
                    f"Review Scraping Done: {num_results}, Progress: {i}/{len(product_ids)}"
                )
        df = pd.DataFrame(sephora_reviews)
        df.to_csv(DIRECTORY + "sephora_reviews.csv", index=False)

        return sephora_reviews

    def scrape_rating(self, export=0):
        print("---------> Started scraping products <---------")
        page = httpx.get(SEPHORA_URL, headers=HEADERS, timeout=300.0)
        soup = BeautifulSoup(page.text, "html.parser")
        DATA["num_pages"], num_products = self.get_pages_num(soup)

        with httpx.Client(
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=50)
        ) as client:
            for k in range(DATA["num_pages"]):
                QUERY = "?currentPage="
                new_url = SEPHORA_URL + QUERY + str(k + 1)
                page = client.get(new_url, headers=HEADERS)
                encoding = chardet.detect(page.content)["encoding"]
                page = page.content.decode(encoding)
                soup = BeautifulSoup(page, "html.parser")
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
                        sephora_rating["url"].append(BASE + product["targetUrl"])
                        current_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        sephora_rating["collected_on"].append(current_time_str)
                print(
                    f'Progress ({round(len(sephora_rating["product_id"])/num_products, 2) * 100}%): {len(sephora_rating["product_id"])}/{num_products}'
                )
                new_url = SEPHORA_URL + QUERY + str(k + 1)
                page = client.get(new_url, headers=HEADERS)
                soup = BeautifulSoup(page.text, "html.parser")
        print("---------> Scraping Complete products <---------")
        if export:
            df = pd.DataFrame(sephora_rating)
            df.to_excel(DIRECTORY + "sephora_rating.xlsx", index=False)
            print(
                f'Sephore scraping done ({round(len(sephora_rating["product_id"])/num_products, 2) * 100}%): {len(sephora_rating["product_id"])}/{num_products}'
            )

        return sephora_rating


if __name__ == "__main__":
    sephora = Sephora()
    sephora.scrape_rating(export=1)
    sephora.scrape_reviews()
    print(f"Duration: {round((time.time() - p), 3)}s") # runtime = 
