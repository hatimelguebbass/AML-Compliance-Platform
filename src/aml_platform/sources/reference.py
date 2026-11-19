"""Reference values for the core banking customer master (Moroccan market)."""

from __future__ import annotations

MALE_FIRST_NAMES: tuple[str, ...] = (
    "Mohamed", "Ahmed", "Youssef", "Hamza", "Omar", "Ayoub", "Mehdi", "Karim", "Rachid", "Hassan",
    "Said", "Abdellah", "Abderrahim", "Abdelaziz", "Mustapha", "Driss", "Khalid", "Nabil", "Anas", "Ismail",
    "Amine", "Adil", "Hicham", "Jamal", "Brahim", "Aziz", "Tarik", "Yassine", "Othmane", "Zakaria",
    "Soufiane", "Imad", "Reda", "Badr", "Ilyas", "Saad", "Noureddine", "Lahcen", "Abdelkader", "Mounir",
    "Fouad", "Hakim", "Larbi", "Taoufik", "Younes", "Marouane", "Walid", "Bilal", "Adam", "Ali",
)  # fmt: skip
FEMALE_FIRST_NAMES: tuple[str, ...] = (
    "Fatima", "Khadija", "Aicha", "Meryem", "Salma", "Imane", "Sara", "Nadia", "Hanane", "Latifa",
    "Zineb", "Houda", "Asmae", "Siham", "Samira", "Naima", "Rachida", "Malika", "Karima", "Loubna",
    "Hajar", "Ghizlane", "Kawtar", "Oumaima", "Hind", "Najat", "Souad", "Btissam", "Wafae", "Laila",
    "Nisrine", "Sanae", "Yasmine", "Rim", "Chaimae", "Hafsa", "Amal", "Ikram", "Nawal", "Bouchra",
)  # fmt: skip
LAST_NAMES: tuple[str, ...] = (
    "El Amrani", "Benali", "Alaoui", "El Idrissi", "Bennani", "Tazi", "Berrada", "El Fassi", "Chraibi", "Lahlou",
    "Bouzidi", "El Ouafi", "Ait Benhaddou", "Ouazzani", "Benjelloun", "Sqalli", "Kettani", "El Mansouri", "Naciri",
    "Zniber", "Belkadi", "Cherkaoui", "El Hajji", "Ziani", "Bakkali", "El Khatib", "Amzil", "Ait Taleb", "Boukhari",
    "Lamrani", "Rahmouni", "El Ghazi", "Haddad", "Filali", "Benkirane", "Sefrioui", "Guessous", "Slaoui", "Iraqi",
    "Tahiri", "El Yazidi", "Moutawakil", "Ennaji", "Hilali", "Bouazza", "El Malki", "Oubaha", "Ait Ali", "Belhaj",
    "Tounsi", "Mernissi", "Abbadi", "El Ouardi", "Benmoussa", "Chafik", "Daoudi", "Fikri", "Hamdaoui", "Jabri",
    "Karimi", "Lyoussi", "Marzouki", "Nouri", "Ouali", "Rami", "Saidi", "Talbi", "Wahbi", "Yacoubi",
    "Zerouali", "Azzouzi", "Bensaid", "Chakir", "Dahbi", "El Bakkali", "Fassi Fihri", "Ghazouani", "Harrak", "Jaafari",
)  # fmt: skip

# Transliteration and spelling variants seen across Arabic-to-Latin records.
NAME_VARIANTS: dict[str, tuple[str, ...]] = {
    "Mohamed": ("Mohammed", "Mohamad", "Mhamed", "Mohammad"),
    "Ahmed": ("Ahmad", "Hmad"),
    "Youssef": ("Youssouf", "Yousef", "Youcef"),
    "Abdellah": ("Abdallah", "Abdel Lah", "Abdullah"),
    "Abderrahim": ("Abdel Rahim", "Abderahim"),
    "Abdelaziz": ("Abdel Aziz", "Abdulaziz"),
    "Mustapha": ("Moustapha", "Mustafa"),
    "Said": ("Saeed", "Saiid"),
    "Ismail": ("Ismael", "Smail"),
    "Zakaria": ("Zakariya", "Zakariae"),
    "Othmane": ("Otmane", "Outhmane", "Othman"),
    "Yassine": ("Yasine", "Yacine"),
    "Fatima": ("Fatna", "Fatimah", "Fatema"),
    "Khadija": ("Khadijah", "Khaddouj"),
    "Aicha": ("Aisha", "Aiche"),
    "Meryem": ("Maryam", "Meriem", "Mariam"),
    "Asmae": ("Asmaa", "Asma"),
    "Chaimae": ("Chaima", "Chaymae"),
    "Oumaima": ("Oumayma", "Omaima"),
    "Ghizlane": ("Ghizlan", "Rhizlane"),
    "El Amrani": ("Elamrani", "Amrani", "El-Amrani"),
    "El Idrissi": ("Elidrissi", "Idrissi", "El-Idrissi"),
    "El Fassi": ("Elfassi", "Fassi"),
    "Ait Benhaddou": ("Ait-Benhaddou", "Aitbenhaddou"),
    "El Mansouri": ("Elmansouri", "Mansouri"),
    "Benjelloun": ("Ben Jelloun", "Benjeloun"),
    "Bennani": ("Ben Nani", "Benani"),
    "Cherkaoui": ("Cherqaoui", "Charkaoui"),
    "El Khatib": ("Elkhatib", "Khatib"),
    "Ouazzani": ("Wazzani", "Ouazani"),
    "Fassi Fihri": ("Fassi-Fihri", "El Fassi Fihri"),
}

# City, CIN letter prefix, postal code, RIB locality code.
CITIES: tuple[tuple[str, str, str, str], ...] = (
    ("Casablanca", "BE", "20000", "780"),
    ("Casablanca", "BK", "20250", "780"),
    ("Rabat", "A", "10000", "810"),
    ("Sale", "AB", "11000", "815"),
    ("Marrakech", "E", "40000", "450"),
    ("Fes", "C", "30000", "270"),
    ("Tanger", "K", "90000", "640"),
    ("Agadir", "J", "80000", "010"),
    ("Meknes", "D", "50000", "490"),
    ("Oujda", "F", "60000", "570"),
    ("Kenitra", "G", "14000", "350"),
    ("Tetouan", "L", "93000", "720"),
    ("El Jadida", "M", "24000", "236"),
    ("Beni Mellal", "I", "23000", "150"),
    ("Nador", "S", "62000", "545"),
    ("Settat", "W", "26000", "700"),
    ("Laayoune", "SH", "70000", "420"),
    ("Mohammedia", "T", "28800", "780"),
)
STREETS: tuple[str, ...] = (
    "Bd Mohammed V", "Bd Zerktouni", "Av Hassan II", "Rue Ibn Battouta", "Av des FAR", "Bd Anfa",
    "Rue Moulay Ismail", "Av Allal El Fassi", "Bd Al Massira", "Rue Oued Sebou", "Av Mohammed VI", "Rue Tarik Ibn Ziad",
)  # fmt: skip
OCCUPATIONS: tuple[str, ...] = (
    "SALARIED_PRIVATE", "CIVIL_SERVANT", "SELF_EMPLOYED", "MERCHANT", "LIBERAL_PROFESSION", "FARMER",
    "STUDENT", "RETIRED", "HOMEMAKER", "UNEMPLOYED", "CRAFTSMAN", "REAL_ESTATE_AGENT",
)  # fmt: skip
COMPANY_WORDS: tuple[str, ...] = (
    "Atlas", "Sahara", "Maghreb", "Anfa", "Sebou", "Tensift", "Rif", "Souss", "Oasis", "Medina", "Argan", "Zellige",
    "Kasbah", "Majorelle", "Bouregreg", "Draa", "Toubkal", "Ifrane", "Mogador", "Andalous",
)  # fmt: skip
COMPANY_ACTIVITIES: tuple[str, ...] = (
    "Trading", "Distribution", "Immobilier", "Logistique", "Textile", "Agro", "Services", "Travaux", "Import Export",
    "Conseil", "Transport", "Negoce", "Industries", "Digital", "Tourisme",
)  # fmt: skip
NATIONALITIES_ABROAD: tuple[str, ...] = ("FR", "ES", "IT", "BE", "NL", "DE", "SN", "CI", "DZ", "TN", "US", "CN")
RESIDENCE_ABROAD: tuple[str, ...] = ("FR", "ES", "IT", "BE", "NL", "DE", "AE", "CA", "US", "GB")
