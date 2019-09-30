'''
    Author:         Brian D. Boyd
    contact:        brian.d.boyd@vanderbilt.edu
    Module name:    Module_edat_limbo2xnat
    Creation date:  2016-10-24
    Purpose:        Upload edat files from LIMBO to XNAT
'''

__author__ = "Brian D. Boyd"
__email__ = "brian.d.boyd@vanderbilt.edu"
__purpose__ = "Upload edat files from LIMBO to XNAT"
__module_name__ = "Module_edat_limbo2xnat"
__modifications__ = "2016-10-24 - Original write"

# Python packages import
import os
import logging
import glob

from dax import XnatUtils, ScanModule

LOGGER = logging.getLogger('dax')

DEFAULT_MODULE_NAME = 'edat_limbo2xnat'

DEFAULT_TMP_PATH = os.path.join('/tmp', DEFAULT_MODULE_NAME)

DEFAULT_TEXT_REPORT = 'ERROR in module ' + DEFAULT_MODULE_NAME + ':\n'

DEFAULT_LIMBO_DIR = '/data/h_taylor/sync-LIMBO'

DEFAULT_MAP = {
    'fMRI_EDP': 'Emotion Dot-Probe 08-30-12-{SUBJ}-*.edat2',
    'fMRI_EmoStroop': 'estroop_DepMIND-{SUBJ}-*.edat2',
    'fMRI_Posner': 'Posner_mANT_DepMIND-{SUBJ}-*.edat2',
    'fMRI_NBack': 'Verbal_N-back*{SUBJ}-*.edat2',
}


class Module_edat_limbo2xnat(ScanModule):
    '''
    Module class for edat_upload that runs on a session

    :param mod_name: module name
    :param directory: temp directory for the module temporary files
    :param email: email address to send error/warning to
    :param text_report: title for the report
    #
    # ADD MORE PARAMETERS AS NEEDED HERE
    #
    '''
    def __init__(
        self,
        mod_name=DEFAULT_MODULE_NAME,
        directory=DEFAULT_TMP_PATH,
        email=None,
        text_report=DEFAULT_TEXT_REPORT,
        limbo=DEFAULT_LIMBO_DIR,
        scan_types='fMRI_EDP,fMRI_Posner,fMRI_NBack,fMRI_EmoStroop',
        scan_map=DEFAULT_MAP
    ):

        super(Module_edat_limbo2xnat, self).__init__(
            mod_name, directory, email, text_report=text_report)
        self.limbo = limbo
        self.scan_map = scan_map
        self.scan_types = XnatUtils.get_input_list(scan_types, None)
        self.xnat = None

    def prerun(self, settings_filename=''):
        '''
        prerun function overridden from base-class
        method that runs at the beginning, before looping
        over the sessions for the project

        :param settings_filename: settings filename to set the temporary folder
        '''
        pass

    def afterrun(self, xnat, project):
        '''
        afterrun function overridden from base-class method that runs
        at the end, after looping over the sessions for the project

        :param xnat: interface to xnat object (XnatUtils.get_interface())
        :param project: project ID/label on XNAT
        '''
        # send report
        if self.send_an_email:
            self.send_report()

    def needs_run(self, cscan, xnat):
        """ needs_run function overridden from base-class
            cscan = CacheScan object from XnatUtils
            return True or False
        """
        _info = cscan.info()
        if _info['type'] not in self.scan_types:
            return False

        # Check for existing EDAT resource
        if XnatUtils.has_resource(cscan, 'EDAT'):
            LOGGER.debug('Has EDAT')
            return False

        return True

    def run(self, scan_info, scan_obj):
        '''
        Find the EDAT file and upload it to the scan
        '''
        limbo_edat = None

        # Find file
        _regex = self.scan_map[scan_info['type']]
        _regex = _regex.format(SUBJ=scan_info['subject_label'])

        limbo_list = self.load_limbo(_regex)
        if len(limbo_list) == 0:
            LOGGER.debug('failed to find edat file')
            return

        # TODO: deal with multiple matches
        if len(limbo_list) == 2 and scan_info['session_label'].endswith('b'):
            limbo_list.sort()
            limbo_list = [limbo_list[1]]

        elif len(limbo_list) > 1:
            LOGGER.debug('multiple edat files found')
            return

        limbo_edat = limbo_list[0]
        limbo_txt = os.path.splitext(limbo_edat)[0] + '.txt'
        limbo_tab = limbo_edat + '_tab.txt'

        if not os.path.exists(limbo_tab):
            LOGGER.warn('tab does not exist:' + limbo_tab)
            return

        if not os.path.exists(limbo_txt):
            LOGGER.warn('txt does not exist:' + limbo_txt)

        # Upload the files
        # for _file in [limbo_edat, limbo_txt, limbo_tab]:
        #    _dst = os.path.basename(_file).replace(" ", "_")
        #    scan_obj.resource('EDAT').file(_dst).put(_file, overwrite=True)
        # Upload _tab.txt only
        _dst = os.path.basename(limbo_tab).replace(" ", "_")
        scan_obj.resource('EDAT').file(_dst).put(limbo_tab, overwrite=True)

    def load_limbo(self, filefilter):
        limbo_list = glob.glob(os.path.join(self.limbo, filefilter))
        return limbo_list
