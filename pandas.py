import pandas as pd

# --- LOAD DATA ---
df = pd.read_csv('employees.csv')

# --- 1. EXPLORATION (Understanding your data) ---
print("--- Data Info ---")
print(df.info())  # Shows column types and if there are null values

print("\n--- Summary Statistics ---")
print(df.describe()) # Shows mean, min, max, etc. for numeric columns

# --- 2. DATA CLEANING ---
# Check for missing values
print("\n--- Missing Values Count ---")
print(df.isnull().sum())

# Fill missing Salary values with the average salary of the company
avg_val = df['Salary'].mean()
df['Salary'] = df['Salary'].fillna(avg_val)

# --- 3. TRANSFORMATION ---
# Convert 'JoinDate' string to actual datetime objects
df['JoinDate'] = pd.to_datetime(df['JoinDate'])

# Rename a column
df = df.rename(columns={'Remote': 'WorkFromHome'})

# --- 4. SORTING & FILTERING ---
# Sort by Salary (highest first)
df_sorted = df.sort_values(by='Salary', ascending=False)

# Filter: Only IT department employees
it_staff = df[df['Department'] == 'IT']

# --- 5. FINAL RESULTS ---
print("\n--- Final Cleaned Dataframe ---")
print(df)

print("\n--- IT Staff Only ---")
print(it_staff)

# Save the cleaned version
# df.to_csv('cleaned_employees.csv', index=False)
