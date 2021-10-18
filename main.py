import os
import crawler as cw

if __name__ == '__main__':
    CACHE = os.environ.get('CACHE') == 'True'
    fina = cw.Fina(CACHE)
    df_results = fina.get_results()
