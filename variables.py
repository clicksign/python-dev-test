VARIABLES = {
    "data_file_path": "data/Adult.data",
    "test_file_path": "data/Adult.test",
    "number_of_columns": 15,
    "expected_header": ["age", "workclass", "fnlwgt", "education",
                        "education num", "marital status", "occupation",
                        "relationship", "race", "sex", "capital gain",
                        "capital loss", "hours per week", "native country",
                        "class"],
    "expected_values_and_types": {
        0: int,
        1: ["Private", "Self-emp-not-inc", "Self-emp-inc",
            "Federal-gov", "Local-gov", "State-gov", "Without-pay",
            "Never-worked"],
        2: int,
        3: ["Bachelors", "Some-college", "11th", "HS-grad", "Prof-school",
            "Assoc-acdm", "Assoc-voc", "9th", "7th-8th", "12th", "Masters",
            "1st-4th", "10th", "Doctorate", "5th-6th", "Preschool"],
        4: int,
        5: ["Married-civ-spouse", "Divorced", "Never-married", "Separated",
            "Widowed", "Married-spouse-absent", "Married-AF-spouse"],
        6: ["Tech-support", "Craft-repair", "Other-service",
            "Sales", "Exec-managerial", "Prof-specialty",
            "Handlers-cleaners", "Machine-op-inspct", "Adm-clerical",
            "Farming-fishing", "Transport-moving", "Priv-house-serv",
            "Protective-serv", "Armed-Forces"],
        7: ["Wife", "Own-child", "Husband", "Not-in-family",
            "Other-relative", "Unmarried"],
        8: ["White", "Asian-Pac-Islander", "Amer-Indian-Eskimo",
            "Other", "Black"],
        9: ["Female", "Male"],
        10: int,
        11: int,
        12: int,
        13: ["United-States", "Cambodia", "England", "Puerto-Rico",
             "Canada", "Germany", "Outlying-US(Guam-USVI-etc)", "India",
             "Japan", "Greece", "South", "China", "Cuba", "Iran",
             "Honduras", "Philippines", "Italy", "Poland", "Jamaica",
             "Vietnam", "Mexico", "Portugal", "Ireland", "France",
             "Dominican-Republic", "Laos", "Ecuador", "Taiwan",
             "Haiti", "Columbia", "Hungary", "Guatemala", "Nicaragua",
             "Scotland", "Thailand", "Yugoslavia", "El-Salvador",
             "Trinadad&Tobago", "Peru", "Hong", "Holand-Netherlands"],
        14: [">50K", "<=50K"],
    },
    "known_wrong_elements": ["?"],
}
