import duckdb

# Connect to DuckDB. If 'my_duckdb.duckdb' does not exist, it will be created.
con = con = duckdb.connect('database/bums_and_roses.duckdb')
import pandas
df=con.execute("select * from final where actual_drop_date<='2024-01-31'").fetch_df()
df.to_csv('for-model.csv', index=False)
con.close()

from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.svm import SVR
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.neural_network import MLPRegressor
from xgboost import XGBRegressor
from catboost import CatBoostRegressor
import pandas as pd
data = pd.read_csv('for-model.csv')
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from joblib import dump, load
from sklearn.base import clone
from sklearn.metrics import mean_absolute_error
import numpy as np

# Selecting relevant columns and excluding non-relevant or target variables for other predictions
features = ['product_type', 'variant', #'product_price', 
            'actual_drop_date',
            'drop_month', 'print', 'season', 'drop_time_holiday', 'print_gender',
            #'product_gender', 
            'main_color', 'designcat', 'designelement',
            'returning_cohort', 'first_time_cohort', 'returning_sales',
            'first_time_sales', 'order_quantity', 
            'confidence_score','convertible_romper_in_drop',
            'pajama_set_in_drop', 'romper_in_drop', 'footie_in_drop', 'prints_in_drop']
target = 'sold_amount_14'

# Dropping rows where the target variable is missing
data = data.dropna(subset=[target])

# Splitting dataset into features and target variable
X = data[features]
y = data[target]

# Handling categorical columns with OneHotEncoder and numerical columns with SimpleImputer and StandardScaler
categorical_features = X.select_dtypes(include=['object', 'bool']).columns
numerical_features = X.select_dtypes(include=['int64', 'float64']).columns

# Creating transformers for both numerical and categorical data
numerical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='mean')),
    ('scaler', StandardScaler())])

categorical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='most_frequent')),
    ('onehot', OneHotEncoder(handle_unknown='ignore'))])

# Combining transformers into a ColumnTransformer
preprocessor = ColumnTransformer(
    transformers=[
        ('num', numerical_transformer, numerical_features),
        ('cat', categorical_transformer, categorical_features)])
X = preprocessor.fit_transform(X)

dump(preprocessor, 'preprocessor/preprocessor.joblib')




# Define models
models = {
    'Linear Regression': LinearRegression(),
    'Ridge': Ridge(),
    'Lasso': Lasso(),
    'ElasticNet': ElasticNet(),
    'SVR': SVR(),
    'Decision Tree': DecisionTreeRegressor(),
    'Random Forest': RandomForestRegressor(n_estimators=100),
    'Gradient Boosting': GradientBoostingRegressor(n_estimators=100),
    'XGBoost': XGBRegressor(n_estimators=100),
    'CatBoost': CatBoostRegressor(n_estimators=100, verbose=False),
    'MLP Neural Network': MLPRegressor(hidden_layer_sizes=(50,), max_iter=500)
}

# Train each model and predict on the test set
predictions = []
for name, model in models.items():
    # Clone model to make sure each iteration uses a fresh model
    cloned_model = clone(model)
    cloned_model.fit(X, y)
    dump(cloned_model, f'model/{name.replace(" ", "_")}.joblib')