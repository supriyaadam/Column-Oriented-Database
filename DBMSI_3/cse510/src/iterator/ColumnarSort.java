package iterator;

import bufmgr.PageNotReadException;
import columnar.ColumnarFile;
import columnar.ColumnarFile;
import global.AttrType;
import global.TupleOrder;
import heap.*;
import index.IndexException;
import value.IntegerValue;
import value.StringValue;
import value.ValueClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static global.AttrType.*;
import static global.AttrType.attrNull;
import static iterator.ColumnarFileScan.getTupleFromPosition;

public class ColumnarSort extends Iterator {

    private ColumnarFileScan columnarFileScan;
    private Sort sort;
    private ColumnarFile columnarfile;


    private AttrType[] projectionAttrs;
    private short[] projectionStrSizes;
    int[] selectedCols;

    public ColumnarSort(
            ColumnarFile columnarfile,
            int[] selectedCols,
            int sort_fld,
            TupleOrder sort_order,
            int n_pages)
            throws InvalidTupleSizeException, HFException, IOException, FieldNumberOutOfBoundException, HFBufMgrException, HFDiskMgrException, FileScanException, TupleUtilsException, InvalidRelation, SortException {

        try {
            this.columnarfile = columnarfile;

            Heapfile sortColHeapFile = columnarfile.getHeapFileColumns()[sort_fld - 1];

            AttrType[] colAttrTypes = columnarfile.getAttributeType();
            AttrType[] sortAttrTypes = new AttrType[1];
            sortAttrTypes[0] = colAttrTypes[sort_fld - 1];

            short[] strSizes = columnarfile.getStrSizes();

            //this.selectedCols = selectedCols;

            this.selectedCols = new int[colAttrTypes.length];
            for (int i = 0; i < colAttrTypes.length; i++) {
                this.selectedCols[i] = i + 1;
            }

            //setProjectionAttrs(colAttrTypes);
            //setProjectionStrSizes(colAttrTypes, strSizes);

            projectionAttrs = colAttrTypes;
            projectionStrSizes = strSizes;


            FldSpec[] projlist = new FldSpec[4];
            RelSpec rel = new RelSpec(RelSpec.outer);
            projlist[0] = new FldSpec(rel, 1);
            projlist[1] = new FldSpec(rel, 2);
            projlist[2] = new FldSpec(rel, 3);
            projlist[3] = new FldSpec(rel, 4);

            String filename = columnarfile.columnarFileName + String.valueOf(sort_fld);


            columnarFileScan = new ColumnarFileScan(columnarfile.columnarFileName, filename, projectionAttrs,
                    strSizes, (short) 4, 4, this.selectedCols, projlist, null, false, columnarfile);

            short sortFieldLen = 0;

            switch (sortAttrTypes[0].attrType) {
                case AttrType.attrInteger:
                    sortFieldLen = 4;
                    break;
                case AttrType.attrString:
                    sortFieldLen = 100;
                    break;
            }
            sort = new Sort(projectionAttrs, (short) 4, projectionStrSizes, columnarFileScan, sort_fld, sort_order, sortFieldLen,
                    10, true);
        }
        catch (RuntimeException ex) {
            ex.printStackTrace();

        }


    }


    private void setProjectionAttrs(AttrType[] columnAttrTypes) {
        projectionAttrs = new AttrType[selectedCols.length];

        for (int idx = 0; idx < projectionAttrs.length; idx++) {
            projectionAttrs[idx] = columnAttrTypes[selectedCols[idx] - 1];
        }
    }


    private void setProjectionStrSizes(AttrType[] columnAttrTypes, short[] columnStrSize) {
        List<Integer> strFields = new ArrayList();

        int selFldsPtr = 0;
        int strSizePtr = 0;

        for (int i = 0; i < columnAttrTypes.length; i++) {
            if (i == selectedCols[selFldsPtr]) {
                if (columnAttrTypes[i].attrType == AttrType.attrString) {
                    strFields.add((int) columnStrSize[strSizePtr++]);
                    selFldsPtr++;
                }
            } else {
                if (columnAttrTypes[i].attrType == AttrType.attrString) {
                    strSizePtr++;
                }
            }
        }

        projectionStrSizes = new short[strFields.size()];
        for (int pos = 0; pos < strFields.size(); pos++) {
            projectionStrSizes[pos] = (short) strFields.get(pos).intValue();
        }
    }

    @Override
    public Tuple get_next()
            throws Exception {

        try {
            Tuple tuple = sort.get_next();

            return tuple;
        }
        catch(Exception ex) {
            columnarFileScan.close();
            sort.close();
            throw ex;
        }
    }


    @Override
    public void close() throws Exception {
        if(sort!=null)
            sort.close();

        if(columnarFileScan !=null)
            columnarFileScan.close();
    }

}
