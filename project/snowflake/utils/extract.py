import pandas as pd

def extract_excel():
    df1 = pd.read_excel('data/excels/weather.xlsx')
    df2 = pd.read_excel('data/excels/maintenance.xlsx')
    df3 = pd.read_excel('data/excels/event.xlsx')
    df4 = pd.read_excel('data/excels/date.xlsx')
    df5 = pd.read_excel('data/excels/timeslot.xlsx')

    df1.to_csv('data/output_csv/weather.csv', index=False)
    df2.to_csv('data/output_csv/maintenance.csv', index=False)
    df3.to_csv('data/output_csv/event.csv', index=False)
    df4.to_csv('data/output_csv/date.csv', index=False)
    df5.to_csv('data/output_csv/timeslot.csv', index=False)

    print("Data extracted and saved as CSV.")

