# #
# Copyright 2018-2018 Ghent University
#
# This file is part of slurm-torque-wrappers
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# All rights reserved.
#
# #

Summary: Slurm wrappers scripts with torque 6 support
Name: slurm-torque-wrappers
Version: 0.2.5
Release: 1

Group: Applications/System
License: GNU Public License v 2.0
URL: htts://github.com/hpcugent/slurm-torque-wrappers
Source0: %{name}-%{version}.tar.gz

BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
BuildArch: x86_64

%description
slurm-torque-wrappers contains an updated version of the wrappers scripts provided by
SchedMD under contribs/torque.

They remove PBSpro only-functionality and provide better (and correct) support for
Torque 6 q-scripts.

%prep
%setup -q


%build


%install
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT/usr/libexec/slurm/wrapper/
install pbsnodes.pl $RPM_BUILD_ROOT/usr/libexec/slurm/wrapper/pbsnodes
install qalter.pl $RPM_BUILD_ROOT/usr/libexec/slurm/wrapper/qalter
install qdel.pl $RPM_BUILD_ROOT/usr/libexec/slurm/wrapper/qdel
install qhold.pl $RPM_BUILD_ROOT/usr/libexec/slurm/wrapper/qhold
install qrerun.pl $RPM_BUILD_ROOT/usr/libexec/slurm/wrapper/qrerun
install qrls.pl $RPM_BUILD_ROOT/usr/libexec/slurm/wrapper/qrls
install qstat.pl $RPM_BUILD_ROOT/usr/libexec/slurm/wrapper/qstat
install qsub.pl $RPM_BUILD_ROOT/usr/libexec/slurm/wrapper/qsub
install mpiexec.pl $RPM_BUILD_ROOT/usr/libexec/slurm/wrapper/mpiexec
install generate_pbs_nodefile.pl $RPM_BUILD_ROOT/usr/libexec/slurm/wrapper/generate_pbs_nodefile

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root)
%attr(755, root, root) %{_libexecdir}/slurm/wrapper/pbsnodes
%attr(755, root, root) %{_libexecdir}/slurm/wrapper/qalter
%attr(755, root, root) %{_libexecdir}/slurm/wrapper/qdel
%attr(755, root, root) %{_libexecdir}/slurm/wrapper/qhold
%attr(755, root, root) %{_libexecdir}/slurm/wrapper/qrerun
%attr(755, root, root) %{_libexecdir}/slurm/wrapper/qrls
%attr(755, root, root) %{_libexecdir}/slurm/wrapper/qstat
%attr(755, root, root) %{_libexecdir}/slurm/wrapper/qsub
%attr(755, root, root) %{_libexecdir}/slurm/wrapper/mpiexec
%attr(755, root, root) %{_libexecdir}/slurm/wrapper/generate_pbs_nodefile


%changelog
* Tue Oct 02 2018 Andy Georges <andy.georges@ugent.be>
- Created spec file
- Initial copy of wrappers
