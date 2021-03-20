
def limpeza():
    import pandas as pd
    import numpy as np

    data = pd.read_csv('https://github.com/Ededu1984/python-dev-test/blob/master/data/Adult.data?raw=true', delimiter=',', header=None,
    names =['age', 'workclass', 'fnlwgt', 'education', 'education-num', 'marital-status', 'occupation', 'relationship', 'race', 'sex', 'capital-gain', 'capital-loss', 'hours-per-week', 'native-country', 'class'])

    test = pd.read_csv('https://github.com/Ededu1984/python-dev-test/blob/master/data/Adult.test?raw=true', delimiter=',', header=None,
    names =['age', 'workclass', 'fnlwgt', 'education', 'education-num', 'marital-status', 'occupation', 'relationship', 'race', 'sex', 'capital-gain', 'capital-loss', 'hours-per-week', 'native-country', 'class'])

    test = test.drop(0, axis=0).reset_index(drop=True)

    data['age'] = data['age'].apply(lambda x: int(x) if x.isnumeric() else 0)
    data['fnlwgt'] = data['fnlwgt'].apply(lambda x: x.split(' ')[1])
    data['fnlwgt'] = data['fnlwgt'].apply(lambda x: float(x) if x.isnumeric() else 0)
    data['capital-gain'] = data['capital-gain'].apply(lambda x: x.split(' ')[1])
    data['capital-gain'] = data['capital-gain'].apply(lambda x: float(x) if x.isnumeric() else 0)
    data['workclass'] = data['workclass'].apply(lambda x: x.split(' ')[1])
    data['workclass'] = data['workclass'].apply(lambda x: x.replace('?', 'null'))
    data['education'] = data['education'].apply(lambda x: x.split(' ')[1])
    data['marital-status'] = data['marital-status'].apply(lambda x: x.split(' ')[1])
    data['occupation'] = data['occupation'].apply(lambda x: x.split(' ')[1])
    data['occupation'] = data['occupation'].apply(lambda x: x.replace('?', 'null'))
    data['relationship'] = data['relationship'].apply(lambda x: x.split(' ')[1])
    data['race'] = data['race'].apply(lambda x: x.split(' ')[1])
    data['sex'] = data['sex'].apply(lambda x: x.split(' ')[1])
    data['native-country'] = data['native-country'].apply(lambda x: x.split(' ')[1])
    data['native-country'] = data['native-country'].apply(lambda x: x.replace('?', 'null'))
    data['class'] = data['class'].apply(lambda x: x.split(' ')[1])
    test['education'] = test['education'].apply(lambda x: x.split(' ')[1])
    test['marital-status'] = test['marital-status'].apply(lambda x: x.split(' ')[1])
    test['occupation'] = test['occupation'].apply(lambda x: x.split(' ')[1])
    test['relationship'] = test['relationship'].apply(lambda x: x.split(' ')[1])
    test['race'] = test['race'].apply(lambda x: x.split(' ')[1])
    test['sex'] = test['sex'].apply(lambda x: x.split(' ')[1])
    test['hours-per-week'] = test['hours-per-week'].apply(lambda x: x.split(' ')[1])
    test['native-country'] = test['native-country'].apply(lambda x: x.split(' ')[1])
    test['class'] = test['class'].apply(lambda x: x.split(' ')[1])
    test['hours-per-week'] = test['hours-per-week'].apply(lambda x: x.split('.')[0])
    test['workclass'] = test['workclass'].apply(lambda x: x.replace('?', 'null'))
    test['occupation'] = test['occupation'].apply(lambda x: x.replace('?', 'null'))
    test['native-country'] = test['native-country'].apply(lambda x: x.replace('?', 'null'))
    test['hours-per-week'] = test['hours-per-week'].apply(lambda x: x.replace('E', '0'))
    df = pd.concat([data, test])
    df = df.reset_index(drop=True)

    df['age'] = df['age'].astype(str)
    df['education-num'] = df['education-num'].astype(str)
    df['age'] = df['age'].apply(lambda x: x.split('.')[0])
    df['education-num'] = df['education-num'].apply(lambda x: x.split('.')[0])
    df['id'] = df.index + 1
    df.to_csv('~/dags/clicksign_df.csv', index=False)