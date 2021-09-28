from setuptools import setup, find_packages

setup(name='paaw',
      version='0.3.9',
      description='KPN package for a Python AEP API Wrapper (paaw)',
      url='https://git.kpn.org/projects/MDF/repos/kpn_cm_aep_api_wraper',
      author='Dennis Hendrikx, Tom Huijdts, Anastasia Khomenko',
      author_email='dennis.hendrikx@kpn.com, tom.huijdts@kpn.com, anastasia.khomenko@kpn.com',
      license='MIT',
      packages=find_packages(),
      package_data={'': ['*.yaml']},
      install_requires=[
          'requests',
          'dictor',
          'pyyaml',
          'jwt',
          'numpy',
          'requests-toolbelt',
          'pybase64',
		  'cryptography==3.4.7',
          'pandas',
          'pyarrow'
      ],
      classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: >=2.7',
      ],
      zip_safe=False,
      include_package_data=True
)
