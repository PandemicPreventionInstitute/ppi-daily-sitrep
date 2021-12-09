chmod +x /mnt/scripts/git_update.sh
cd /mnt
cp -r scripts/* ../repos/ppi-daily-sitrep/scripts/.
cd /repos/ppi-daily-sitrep
git remote set-url origin https://rkassa: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@github.com/PandemicPreventionInstitute/ppi-daily-sitrep.git
git config --global user.email "rkassa@rockfound.org"
git config --global user.name "Robel Kassa"
git pull
git rm scripts/git_update.sh
git add .
git commit -m 'scheduled script update from domino'
git push