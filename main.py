import os
import sys
import logging
import threading
from queue import Queue
from datetime import date

import requests
from bs4 import BeautifulSoup
from sqlalchemy import Column, Integer, String
from sqlalchemy import Table, MetaData, create_engine

JOBS = [
    "59702994", "59700291", "59685698",
    "59718549", "59710383", "59709363",
    "59704904", "59712101", "59701412",
    "59704904"
]

class EJScrapper:
    if not os.path.exists("./logs/"):os.makedirs("./logs/")
    logging.basicConfig(level=logging.INFO, 
        format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
        handlers=[logging.FileHandler(f"./logs/ej_scraper_log_{date.today()}.log"), 
        logging.StreamHandler(sys.stdout)])
    logging.info(" ========= External Jobs Scraper Started =========")

    def __init__(self) -> None:
        if not os.path.exists("./data/"):os.makedirs("./data/")
        self.engine = create_engine('sqlite:///data/jobs.db', future=True)
        self.crawled, self.ej_jobs_set = set(), set()
        self.queue = Queue()

    def __test_if_external(self, job_id:int) -> tuple:
        """Test if a job is external"""
        logging.info(f"Checking if job with id {job_id} is external")
        response = requests.get(f"https://www.seek.com.au/job/{job_id}")
        soup = BeautifulSoup(response.text, "html.parser")
        apply_link = soup.find("a", {"data-automation":"job-detail-apply"})
        if "linkout" in apply_link["href"]:return True, soup
        else:return False, soup

    def __get_job_details(
        self, id_:int, ej_table:Table, soup:BeautifulSoup) -> None:
        """Fetch job title and company name"""
        logging.info(f"External job found! >> Job_Id: {id_}")
        self.ej_jobs_set.add(id_)
        title_tag = soup.find("h1", {"data-automation":"job-detail-title"})
        title = title_tag.text.strip().replace("\n", "")
        company_name_tag = soup.find("span", {"data-automation":"advertiser-name"})
        company_name = company_name_tag.find("a").text.strip().replace("\n", "")
        self.__update_table(title, company_name, id_, ej_table)

    def __create_table(self) -> Table:
        """create table for saving external jobs"""
        meta = MetaData()
        external_jobs = Table(
            "e_jobs", meta,
            Column("id", Integer, primary_key=True),
            Column("job_title", String),
            Column("company", String)
        )
        meta.create_all(self.engine)
        return external_jobs

    def __update_table(
        self, j_title:str, c_name:str, id_:str, ej_table:Table) -> None:
        """Add record to table"""
        with self.engine.connect() as conn:
            conn.execute(ej_table.insert().values(
                id=id_, job_title=j_title, company=c_name
            ))
            conn.commit()
    
    def work(self) -> None:
        while True:
            job_id, ej_table = self.queue.get()
            job_is_external, soup = self.__test_if_external(job_id)
            if job_is_external:
                self.__get_job_details(job_id, ej_table, soup)
            self.crawled.add(job_id)
            logging.info(f"{threading.current_thread().name}>> "
                f"Crawled: {len(self.crawled)} ||"
                f" Total External Jobs: {len(self.ej_jobs_set)}"
            )
            self.queue.task_done()
    
    def create_thread_jobs(self, job_ids:list, ej_table:Table) -> None:
        [self.queue.put((job_id, ej_table)) for job_id in job_ids]
        self.queue.join()

    def run(self) -> None:
        ej_table = self.__create_table()
        [threading.Thread(target=self.work, daemon=True).start()
        for _ in range(10)]
        self.create_thread_jobs(JOBS, ej_table)
    
if __name__ == "__main__":
    scraper = EJScrapper()
    scraper.run()