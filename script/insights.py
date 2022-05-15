from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import OrdinalEncoder
import pandas as pd

from script.load import insert_data

def get_feature_importance(model, original_column_name):
    """
    Get the feature importance of a model. 
    Return: Dictionary with features importance
    """
    
    feature_importances = pd.DataFrame(model.feature_importances_,
                                        index=model.feature_names_in_,
                                        columns=['importance']).sort_values('importance', ascending=False)

    importances = { feature : 0 for feature in original_column_name }

    for index, row in feature_importances.iterrows():
        feature_name = index.strip()
        curr_importance = row[0]
        
        for feature in original_column_name:
            if feature.startswith(feature_name):
                importances[feature] += curr_importance
                break
    
    return importances


def get_importance_of_attributes_to_class(dataset, _class):
    # Split dataset into x and y (Feature and label)
    x, y = dataset.drop(_class, axis=1), dataset[_class]

    # Create a random forest classifier 
    # (Choose Random Forest insted of Decision Tree because it can be more assertive)
    clf = RandomForestClassifier()

    # Create original encoder (Convert label column to 0 and 1 and fit to it)
    enc = OrdinalEncoder()

    # OriginalEncoder only accepts array, so we need to convert it from pandas dataset
    y = enc.fit_transform(y.values.reshape(-1, 1)).ravel()

    # Reverse columns order, so column name like 'education_num' gets checked before 'education'.
    # Since we need the prefix, 'education' might also accumulate the feature importance 
    # of 'education_num'
    original_column_names = list(x.columns)

    original_column_names = sorted(original_column_names, reverse=True)

    # Apply one-hot-encoding to the dataset
    # Dummies are created for each column in the dataset (they change to binary columns)
    # and then concatenated with original columns prefix name
    x = pd.get_dummies(x)

    clf.fit(x,y)

    feat_importances = get_feature_importance(clf, original_column_names)

    print('Importances of attributes to how much money an adult would make:')
    for f, i in feat_importances.items():
        print(f'{f} : {round(i*100, 2)}%')

    # Create new table with the feature importance, so it can be used in a dashboard
    feat_importances[_class] = 'Prediction Label'
    importance = pd.DataFrame([feat_importances])
    insert_data("feature_importance", importance)