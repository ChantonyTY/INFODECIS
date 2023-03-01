# Exporter Scala
export PATH=/usr/gide/sbt-1.3.13/bin:$PATH

# Pour compiler le projet
sbt clean compile

# Pour exécuter le projet
sbt run

# Pour git
echo "# INFODECIS" >> README.md
git init
git add README.md | git add *
git commit -m "first commit"
git branch -M main
git remote add origin https://github.com/ChantonyTY/INFODECIS.git
git push -u origin main

# TOKEN GIT
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
