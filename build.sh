TOOL=export
export LD_LIBRARY_PATH=/usr/local/lib
pyinstaller --hidden-import wekalib --onefile ${TOOL}.py

TARGET=tarball/$TOOL
mkdir -p $TARGET
cp dist/$TOOL $TARGET
cp ${TOOL}.yml $TARGET
cd tarball
tar cvzf ../${TOOL}.tar $TOOL

