from dataclasses import dataclass


@dataclass
class Account:
    name: str
    ads_account_id: str


accounts = [
    Account("Ballpoint Marketing", "1000386743838958"),
    Account("Call Porter", "1162535854578775"),
    Account("Create Cash Flow", "10152096239569121"),
]
