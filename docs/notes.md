# Run Tests

```bash
$ cd /Users/caine/workspace/sqlpt
$ python3 -m unittest tests.test_classes
$ python3 -m unittest tests.test_other
```


# Publish Package to PyPi

## Install Needed Tools
```bash
$ python -m pip install build twine
$ brew install tree
```

## Build Package Files
```bash
$ cd /Users/caine/workspace/sqlpt
$ python -m build
```

## Verify Package Files
```bash
$ cd dist/
$ unzip sqlpt-0.2.0-py3-none-any.whl -d sqlpt-whl
$ tree sqlpt-whl/
$ twine check dist/*
```

## Deploy to Test PyPi
Before deploying, make sure dist/ only contains the newly versioned files. If there are
old files, remove them.
```bash
$ twine upload -r testpypi dist/*
```

## Install from Test PyPi
```bash
$ python -m pip install -i https://test.pypi.org/simple sqlpt
```

## Deploy to Real PyPi
```bash
$ twine upload dist/*
```

## Install from Real PyPi
```bash
$ python -m pip install sqlpt
```
