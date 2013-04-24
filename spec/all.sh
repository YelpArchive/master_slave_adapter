#!/usr/bin/env bash

source `which rvm | sed 's/rvm\/bin/rvm\/scripts/'`

for ruby in 1.8.7 1.9.2 1.9.3 ree 2.0.0; do
  rvm use $ruby

  for gemfile in spec/gemfiles/*; do
    if [[ "$gemfile" =~ \.lock ]]; then
      continue
    fi

    # skip unsupported combinations
    if [[ "$ruby" == "2.0.0" && "$gemfile" =~ "2.3" ]]; then
      continue
    fi

    echo "Testing $ruby @ $gemfile"

    BUNDLE_GEMFILE=$gemfile bundle install --quiet
    BUNDLE_GEMFILE=$gemfile bundle exec rake spec
  done
done
