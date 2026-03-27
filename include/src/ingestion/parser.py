from bs4 import BeautifulSoup


class TapologyParser:
    def __init__(self, html_content: str):
        self.soup = BeautifulSoup(html_content, "html.parser")

    def parse_fighter_details(self, fighter_name: str) -> dict:
        details = {"Fighter Name": fighter_name}
        container = self.soup.find("div", id="standardDetails")
        if container:
            for label in container.find_all("strong"):
                key = label.get_text(strip=True).replace(":", "")
                value_node = label.find_next_sibling(["span", "a"])
                details[key] = value_node.get_text(strip=True) if value_node else "N/A"
        return details

    def parse_fight_results(self, fighter_name: str) -> list:
        results = []
        bouts = self.soup.find_all("div", attrs={"data-bout-id": True})
        for bout in bouts:
            # Sua lógica de extração de lutas...
            item = {"Main Fighter": fighter_name, "Status": "N/A"}  # etc...
            results.append(item)
        return results
