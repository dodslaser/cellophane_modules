# Cellophane Modules

This is the main repo containing all public cellophane modules. Modules aim to be as generalized as possible for maximum reusability.

## Using a module

Modules can be added to a cellophane wrapper as subtrees or submodules in the `modules` directory.

```shell
git remote add modules "https://github.com/ClinicalGenomicsGBG/cellophane_modules"
git subtree add modules my_module --prefix modules/my_module --squash -m "Add my_module"
```

To update a module simply pull the subtree

```shell
git subtree pull modules my_module --prefix modules/my_module --squash -m "Update my_module"
```

Hooks, Mixins and Runners are automatically detected by cellophane on runtime. Configuration schemas will be automatically merged, and configuration options made available. 

## Adding a new module to the repo

To add a module to this repo the module needs to be added to the branch_modules workflow. This will create a separate branch for each module which can then be added by cellophane wrappers.