import pandas as pd




if __name__ == '__main__':
    data = pd.read_csv('stock_data.csv')
    print(data.shape)

