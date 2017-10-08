Name:           plproxy
Version:        %{major_version}
Release:        %{minor_version}%{?dist}
Summary:        PostgreSQL partitioning system

Group:          Applications/Databases
License:        ISC
Source0:        plproxy-rpm-src.tar.gz
Requires:       postgresql-server
BuildRequires:  flex, bison

%description
PL/Proxy is a database partitioning system implemented as PL language.  The
main idea is that proxy function will be created with same signature as
remote function to be called, so only destination info needs to be specified
inside proxy function body.

%prep
%setup -q -n %{name}

%build
make

%install
rm -rf %{buildroot}
make install DESTDIR=%{buildroot}

%clean
rm -rf %{buildroot}

%check
EXTVERSION=%{ext_version} ./runtests.sh

%files
%defattr(-,root,root,-)
%doc AUTHORS COPYRIGHT NEWS README doc/*
%{_libdir}/pgsql/plproxy.so
%{_datadir}/pgsql/extension/plproxy*

%changelog
* Wed Aug 21 2013 Oskari Saarenmaa <os@ohmu.fi> - 2.5-8.g294e9a5
- Initial.
