from calendar import monthrange
from datetime import datetime, timedelta
import os
from flo.computation import Computation
from flo.subprocess import check_call
from flo.time import TimeInterval
from flo.util import augmented_env, symlink_inputs_to_working_dir
from flo.sw.hirs_tpw_daily import HIRS_TPW_DAILY


class HIRS_TPW_MONTHLY(Computation):

    parameters = ['granule', 'sat', 'hirs_version', 'collo_version', 'csrb_version', 'ctp_version',
                  'tpw_version']
    outputs = ['out']

    def build_task(self, context, task):

        num_days = monthrange(context['granule'].year, context['granule'].month)[1]
        interval = TimeInterval(context['granule'], context['granule'] + timedelta(num_days),
                                False, True)

        daily_contexts = HIRS_TPW_DAILY().find_contexts(context['sat'], context['hirs_version'],
                                                        context['collo_version'],
                                                        context['csrb_version'],
                                                        context['ctp_version'],
                                                        context['tpw_version'], interval)

        for (i, c) in enumerate(daily_contexts):
            task.input('TPWD-{}'.format(i), HIRS_TPW_DAILY().dataset('out').product(c))

    def run_task(self, inputs, context):

        inputs = symlink_inputs_to_working_dir(inputs)
        lib_dir = os.path.join(self.package_root, context['tpw_version'], 'lib')

        output = 'tpw.monthly.hirs.{}.{}.{}.ssec.nc'.format(context['sat'],
                                                            context['granule'].strftime('D%y%j'),
                                                            context['tpw_version'])

        # Generate TPW Daily Input List
        tpw_daily_file = 'tpw_daily_list'
        with open(tpw_daily_file, 'w') as f:
            [f.write('{}\n'.format(input)) for input in inputs.values()]

        cmd = os.path.join(self.package_root, context['tpw_version'],
                           'bin/create_monthly_daynight_tpw.exe')
        cmd += ' {} {}'.format(tpw_daily_file, output)

        print cmd
        check_call(cmd, shell=True, env=augmented_env({'LD_LIBRARY_PATH': lib_dir}))

        return {'out': output}

    def find_contexts(self, sat, hirs_version, collo_version, csrb_version, ctp_version,
                      tpw_version, time_interval):

        granules = []

        start = datetime(time_interval.left.year, time_interval.left.month, 1)
        end = datetime(time_interval.right.year, time_interval.right.month, 1)
        date = start

        while date <= end:
            granules.append(date)
            date = date + timedelta(days=monthrange(date.year, date.month)[1])

        return [{'granule': g, 'sat': sat, 'hirs_version': hirs_version,
                 'collo_version': collo_version,
                 'csrb_version': csrb_version,
                 'ctp_version': ctp_version,
                 'tpw_version': tpw_version}
                for g in granules]

    def context_path(self, context, output):

        return os.path.join('HIRS',
                            '{}/{}'.format(context['sat'], context['granule'].year),
                            'TPW_MONTHLY')
