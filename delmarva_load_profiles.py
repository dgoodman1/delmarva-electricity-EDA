
# coding: utf-8

# PURPOSE:
# Generlize dowloading and parsing Delmarva load profile data.
#
# NOTES:
# * In Oracle, LP 98 has T_DMS_LOAD_PROFILE MDDLG; however, Delmara's segment
# is named MDDGL.  This has been changed in
#   the code mapping file.

import os
import pandas as pd
import requests
import numpy as np


class Delmarva:
    """
    Download and parse load profile data.  Export for upload to Oracle.

    Example Usage
    -------------
    >>> from delmarva_load_profiles import Delmarva
    >>> dpl = Delmarva()
    >>> lp = dpl.get_lp_data('1/1/2017', '1/10/2017')
    >>> dpl.export_for_oracle(lp)

    """
    def __init__(self, codes_path=None, dest_path=None):
        self.dest_path = dest_path or os.getcwd()
        self.ldc_state = {'CNM': 'MD', 'CND': 'DE', 'CNV': 'VA'}
        self.delmarva_url = ("http://www2.conectiv.com/cpd/tps/archives/"
                             "{state_lower}/{yr}/{mo}/{sDt}{state_upper}"
                             "A{url_index}.txt")
        self.codes_path = codes_path or os.getcwd()
        self.codes_file_name = 'delmarva_lp_code_mapping.csv'
        file_path = self.codes_path + os.path.sep + self.codes_file_name
        if os.path.exists(file_path):
            self.codes = pd.read_csv(file_path, index_col='T_DMS_LOAD_PROFILE')
        else:
            print('WARNING: Codes file not found: {}'.format(file_path))
            self.codes = pd.DataFrame()
        self.ldcs = ['CND', 'CNM']

    def _retrieve_raw_data(self, dt, ldc):
        """
        Download raw data as text from DPL's website. Prioritizes updated
        data if available.
        """

        # Check LDC and date are valid
        assert ldc in self.ldcs
        if not all(hasattr(dt, a) for a in ('month', 'year')):
            raise TypeError("Invalid date type: {}.  Must have month and "
                            "year attributes.".format(type(dt)))

        for i in (2, 1):
            params = {'yr': dt.year,
                      'mo': str(dt.month).zfill(2),
                      'sDt': dt.strftime('%Y%m%d'),
                      'state_lower': self.ldc_state[ldc].lower(),
                      'state_upper': self.ldc_state[ldc].upper(),
                      'url_index': i}
            r = requests.get(self.delmarva_url.format(**params))

            if r.status_code == 200:
                return r.text
        else:
            r.raise_for_status()

    def get_lp_series(self, target_dt, ldc, agg_dst_hr2=True):
        """
        Download Delmarva load profile data from website.
        Validate and convert raw text to a structured DataFrame.
        
        Parameters
        ----------
        target_dt : (date) Day's data to download.
        ldc : (str) Three character code name for LDC.  Use `CNM`
            for Delmarva Power Maryland and `CND` for Delmarva
            Power Delaware.
        agg_dst_hr2 : (bool) Aggregate the two Hour 2 observations
            on the Fall DST transition date if True; else return
            both columns unaggregated.
        
        """

        try:
            data = self._retrieve_raw_data(target_dt, ldc)
        except requests.exceptions.HTTPError:
            print("Warning: No data available for {} on {:%b %d, %Y}."
                  .format(ldc, target_dt))
            return

        # Split out raw data into list of lists
        lines = data.replace('\r', '').split('\n')
        parsed = [line.split() for i, line in enumerate(lines)
                  if len(line) > 1 and i > 3]
        n_lines = len(parsed)

        if n_lines < 2:
            if not n_lines:
                raise ValueError("Did not receive any valid data for "
                                 "{:%b %d, %Y}".format(target_dt))
            else:
                print("WARNING: Did not receive data for all load profiles "
                      "for {:%b %d, %Y}".format(target_dt))

        # Set up columns
        n_cols = len(parsed[0])
        dst = (n_cols == 27)
        hours = ['H' + str(h).zfill(2) for h in range(1, 25)]
        cols = ['segment', 'date']

        # Account for DST structure
        if dst:
            hours.insert(2, 'H02X')
        elif n_cols != 26:
            raise ValueError("Invalid data structure returned.  Data "
                             "set does not have 26 or 27 columns.")
        
        cols += hours
        df = pd.DataFrame(parsed, columns=cols)

        # Set up functions to convert data types
        dtypes = dict.fromkeys(hours, lambda x: x.astype('float'))
        dtypes['date'] = lambda dt: pd.to_datetime(dt, format='%m/%d/%Y')
        dtypes['segment'] = lambda s: s.str.strip().str.slice(start=8)
        
        # Convert data formats
        for c, f in dtypes.items():
            df[c] = f(df[c])

        # Combine DST Hour 2
        if dst and agg_dst_hr2:
            df['H02'] += df['H02X']
            del df['H02X']

        return df

    def get_lp_data(self, from_dt, to_dt):
        """Get all available load profile data series in date range for CNM and CND."""
        if isinstance(from_dt, str):
            from_date, to_date = pd.to_datetime((from_dt, to_dt))
        else:
            from_date, to_date = from_dt, to_dt

        rng = pd.date_range(from_date, to_date, freq='D')
        
        return pd.concat([self.get_lp_series(dt, ldc)
                          for dt in rng for ldc in self.ldcs],
                         ignore_index=True)

    def export_for_oracle(self, lp_data, path=None, file_name=None):
        """
        Export load profile data in required format for upload to Oracle.

        Only uses load profiles in codes file.  File will automatically be
        uploaded using default path.

        Parameters
        ----------
        lp_data : (DataFrame) Load profile data with rows as day, load profile
            combination and hourly observations as columns (features).
        path : Specify path if not using default (databases/Conectiv).
        file_name : Specify file name if not using default (Conectiv_<date>.txt).
        """
        path = path or self.dest_path
        from_date, to_date = lp_data.date.min(), lp_data.date.max()
        
        if not file_name:
            file_name = 'Conectiv_{:%Y%m%d}.txt'.format(from_date)
        
        if os.path.isdir(path + 'Archive') and \
            os.path.exists(path + 'Archive\\' + file_name):
                raise FileExistsError("File '{}' already in Archive.  Must "
                                      "choose an alternative name for file "
                                      "to upload to Oracle."
                                      .format(file_name))
        (lp_data
         .set_index('segment')
         .join(self.codes.C_LOAD_PROFILE, how='right')
         .reset_index(drop=True)
         .set_index('C_LOAD_PROFILE')
         .assign(EMPTY=np.nan)
         .sort_values('date')
         .sort_index()
         .to_csv(path + file_name, sep='\t', header=False,
                 date_format='%m/%d/%Y', float_format='%.3f'))
        params = (from_date, to_date, path + file_name)
        print("Delmarva load profile data for {:%b %d, %Y} to {:%b %d, %Y}"
              "has been downloaded to {}.".format(*params))