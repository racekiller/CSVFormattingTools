def (df, path):
    import seaborn as sns
    import matplotlib.pyplot as plt
    %matplotlib inline

    # df['Ticks'] = range(0,len(df.index.values))
    StdDevAll = df.std(ddof=0).sort_values()
    StdDevAll_Below_1 = StdDevAll[StdDevAll<1]
    StdDevAll_Below_1_df = pd.DataFrame(StdDevAll_Below_1).reset_index()
    StdDevAll_Below_1_df.columns = ['Sensors', 'Values']
    StdDevAll_Below_1_df = StdDevAll_Below_1_df.set_index('Sensors').T.columns

    for i in range(len(StdDevAll_Below_1_df)):
        plt.plot_date(x=df3['DATETIME'], y=df3[StdDevAll_Below_1_df[i]], fmt="r-")
        plt.xlabel('DATETIME')
        plt.ylabel(StdDevAll_Below_1_df[i])
        plt.title('Instrument ' + df3.columns[i] + ' Process Data')
        plt.savefig(path + StdDevAll_Below_1_df[i] +'.png', transparent = False, bbox_inches = 'tight', dpi = 300)
        plt.show()
    return (StdDevAll_Below_1_df)